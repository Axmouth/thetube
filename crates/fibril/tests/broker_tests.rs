use std::sync::Arc;

use anyhow::Context;
use fibril_broker::coordination::*;
use fibril_broker::*;
use fibril_metrics::Metrics;
use fibril_storage::*;
use fibril_util::unix_millis;
use tokio::task::JoinHandle;

// TODO: make shared
fn make_test_store() -> anyhow::Result<impl Storage> {
    // make testdata dir
    std::fs::create_dir_all("test_data")?;
    // make random temp filename to avoid conflicts
    let filename = format!("test_data/{}", fastrand::u64(..));
    Ok(make_rocksdb_store(&filename, false)?)
}

async fn make_test_broker() -> anyhow::Result<Broker<NoopCoordination>> {
    let store = make_test_store()?;
    let metrics = Metrics::new(60 * 60);
    Ok(Broker::try_new(
        store,
        NoopCoordination,
        metrics.broker(),
        BrokerConfig {
            cleanup_interval_secs: 150,
            inflight_ttl_secs: 3,
            publish_batch_size: 64,
            publish_batch_timeout_ms: 1,
            ack_batch_size: 64,
            ack_batch_timeout_ms: 1,
            inflight_batch_size: 64,
            inflight_batch_timeout_ms: 1,
            reset_inflight: false,
        },
    )
    .await?)
}

async fn make_test_broker_with_cfg(
    config: BrokerConfig,
) -> anyhow::Result<Broker<NoopCoordination>> {
    let store = make_test_store()?;
    let metrics = Metrics::new(60 * 60);
    Ok(Broker::try_new(store, NoopCoordination, metrics.broker(), config).await?)
}

fn make_default_cons_cfg() -> ConsumerConfig {
    ConsumerConfig::default().with_prefetch_count(100)
}

#[tokio::test]
async fn broker_basic_delivery() -> anyhow::Result<()> {
    let broker = make_test_broker().await?;

    // Publish 3 messages
    broker.publish("t1", b"hello").await?;
    broker.publish("t1", b"world").await?;
    broker.publish("t1", b"!").await?;

    // Subscribe
    let mut consumer = broker
        .subscribe("t1", "g1", make_default_cons_cfg())
        .await?;

    let mut received = vec![];
    for _ in 0..3 {
        let msg = consumer
            .messages
            .recv()
            .await
            .context("receiving message")?;
        received.push(String::from_utf8(msg.message.payload.clone())?);
        consumer
            .acker
            .send(AckRequest {
                delivery_tag: msg.delivery_tag,
            })
            .await?;
    }

    assert_eq!(received, ["hello", "world", "!"]);
    Ok(())
}

#[tokio::test]
async fn broker_ack_behavior() -> anyhow::Result<()> {
    let broker = make_test_broker().await?;

    broker.publish("t", b"a").await?;
    broker.publish("t", b"b").await?;

    let mut consumer = broker.subscribe("t", "g", make_default_cons_cfg()).await?;

    // Receive & ack first message
    let m1 = consumer
        .messages
        .recv()
        .await
        .context("receiving message")?;
    assert_eq!(m1.message.payload, b"a");
    consumer
        .acker
        .send(AckRequest {
            delivery_tag: m1.delivery_tag,
        })
        .await?;

    // Receive & ack second
    let m2 = consumer
        .messages
        .recv()
        .await
        .context("receiving message")?;
    assert_eq!(m2.message.payload, b"b");
    consumer
        .acker
        .send(AckRequest {
            delivery_tag: m2.delivery_tag,
        })
        .await?;

    // Should not receive anything else now
    tokio::time::sleep(std::time::Duration::from_millis(3)).await;
    assert!(consumer.messages.try_recv().is_err());
    Ok(())
}

#[tokio::test]
async fn broker_work_queue_distribution() -> anyhow::Result<()> {
    let broker = make_test_broker().await?;

    // Publish some work
    for i in 0..6 {
        broker
            .publish("jobs", format!("job-{i}").as_bytes())
            .await?;
    }

    // Two consumers, same group (competing consumers)
    let mut c1 = broker
        .subscribe("jobs", "workers", make_default_cons_cfg())
        .await?;
    let mut c2 = broker
        .subscribe("jobs", "workers", make_default_cons_cfg())
        .await?;

    let mut got1 = 0;
    let mut got2 = 0;

    // Collect 6 messages total (in any order)
    for _ in 0..6 {
        tokio::select! {
            Some(msg) = c1.messages.recv() => {
                got1 += 1;
                c1.acker.send(AckRequest { delivery_tag: msg.delivery_tag }).await?;
            }
            Some(msg) = c2.messages.recv() => {
                got2 += 1;
                c2.acker.send(AckRequest { delivery_tag: msg.delivery_tag }).await?;
            }
            else => anyhow::bail!("all consumers closed unexpectedly"),
        }
    }

    // Work should be distributed, not replicated
    assert_eq!(got1 + got2, 6);
    assert!(got1 > 0);
    assert!(got2 > 0);
    Ok(())
}

#[tokio::test]
async fn broker_pubsub_multiple_groups() -> anyhow::Result<()> {
    let broker = make_test_broker().await?;

    broker.publish("events", b"alpha").await?;
    broker.publish("events", b"beta").await?;

    let mut g1 = broker
        .subscribe("events", "g1", make_default_cons_cfg())
        .await?;
    let mut g2 = broker
        .subscribe("events", "g2", make_default_cons_cfg())
        .await?;

    let mut recv_g1 = vec![];
    let mut recv_g2 = vec![];

    for _ in 0..2 {
        let m1 = g1.messages.recv().await.context("receiving message")?;
        recv_g1.push(String::from_utf8(m1.message.payload.clone())?);
        g1.acker
            .send(AckRequest {
                delivery_tag: m1.delivery_tag,
            })
            .await?;

        let m2 = g2.messages.recv().await.context("receiving message")?;
        recv_g2.push(String::from_utf8(m2.message.payload.clone())?);
        g2.acker
            .send(AckRequest {
                delivery_tag: m2.delivery_tag,
            })
            .await?;
    }

    assert_eq!(recv_g1, ["alpha", "beta"]);
    assert_eq!(recv_g2, ["alpha", "beta"]);
    Ok(())
}

#[tokio::test]
async fn broker_delivery_in_order() -> anyhow::Result<()> {
    let broker = make_test_broker().await?;

    for i in 0..10 {
        broker
            .publish("numbers", format!("{}", i).as_bytes())
            .await?;
    }

    let mut consumer = broker
        .subscribe("numbers", "g", make_default_cons_cfg())
        .await?;

    let mut collected = vec![];

    for _ in 0..10 {
        let msg = consumer
            .messages
            .recv()
            .await
            .context("receiving message")?;
        collected.push(String::from_utf8(msg.message.payload.clone())?);
        consumer
            .acker
            .send(AckRequest {
                delivery_tag: msg.delivery_tag,
            })
            .await?;
    }

    assert_eq!(
        collected,
        (0..10).map(|i| i.to_string()).collect::<Vec<_>>()
    );
    Ok(())
}

#[tokio::test]
async fn broker_consumer_drop_stops_delivery() -> anyhow::Result<()> {
    let broker = make_test_broker().await?;

    broker.publish("t", b"x").await?;

    let consumer = broker.subscribe("t", "g", make_default_cons_cfg()).await?;

    // Drop receiver
    drop(consumer.messages);

    // Delivery loop should stop without panicking
    tokio::time::sleep(std::time::Duration::from_millis(3)).await;

    // Publishing more messages should not cause issues
    assert!(broker.publish("t", b"y").await.is_ok());
    Ok(())
}

#[tokio::test]
async fn redelivery_occurs_after_expiration() -> anyhow::Result<()> {
    let broker = make_test_broker().await?;

    // Publish 1 message
    broker.publish("topic", b"hello").await?;

    // Subscribe
    let mut cons = broker
        .subscribe("topic", "g", make_default_cons_cfg())
        .await?;

    // First delivery
    let m1 = cons.messages.recv().await.context("receiving message")?;
    assert_eq!(m1.message.payload, b"hello");

    // Do NOT ack → let it expire
    tokio::time::sleep(std::time::Duration::from_secs(4)).await; // >3s TTL

    // Message should be redelivered
    let m2 = cons.messages.recv().await.context("receiving message")?;
    assert_eq!(m2.message.offset, m1.message.offset);
    assert_eq!(m2.message.payload, b"hello");

    // Now ACK to finish
    cons.acker
        .send(AckRequest {
            delivery_tag: m2.delivery_tag,
        })
        .await?;
    Ok(())
}

#[tokio::test]
async fn ack_prevents_redelivery() -> anyhow::Result<()> {
    let broker = make_test_broker().await?;

    broker.publish("topic", b"hi").await?;

    let mut cons = broker
        .subscribe("topic", "g", make_default_cons_cfg())
        .await?;

    let msg = cons.messages.recv().await.context("receiving message")?;
    cons.acker
        .send(AckRequest {
            delivery_tag: msg.delivery_tag,
        })
        .await?;

    // Wait past expiration window
    tokio::time::sleep(std::time::Duration::from_secs(4)).await;

    // Should NOT redeliver
    assert!(cons.messages.try_recv().is_err());

    drop(cons);
    drop(broker);
    Ok(())
}

#[tokio::test]
async fn redelivery_load_balanced_to_consumers() -> anyhow::Result<()> {
    let broker = make_test_broker().await?;

    broker.publish("t", b"x").await?;

    let mut c1 = broker.subscribe("t", "g", make_default_cons_cfg()).await?;
    let mut c2 = broker.subscribe("t", "g", make_default_cons_cfg()).await?;

    let m = c1.messages.recv().await.context("receiving message")?;
    assert_eq!(m.message.payload, b"x");

    // Let it expire
    tokio::time::sleep(std::time::Duration::from_secs(4)).await;

    // Should go to any consumer — we just verify it redelivers
    let redelivered = tokio::select! {
        Some(m2) = c1.messages.recv() => m2,
        Some(m2) = c2.messages.recv() => m2,
        else => anyhow::bail!("all consumers closed unexpectedly"),
    };

    assert_eq!(redelivered.message.payload, b"x");
    Ok(())
}

#[tokio::test]
async fn selective_ack_out_of_order() -> anyhow::Result<()> {
    let broker = make_test_broker().await?;

    // Publish messages 0..4
    for i in 0..5 {
        broker.publish("t", format!("m{i}").as_bytes()).await?;
    }

    let mut cons = broker.subscribe("t", "g", make_default_cons_cfg()).await?;

    // Receive messages
    let mut messages = vec![];
    for _ in 0..5 {
        let msg = cons.messages.recv().await.context("receiving message")?;
        messages.push(msg);
    }

    // ACK out-of-order: ACK offset 3 and 4 first
    cons.acker
        .send(AckRequest {
            delivery_tag: messages[3].delivery_tag,
        })
        .await?;
    cons.acker
        .send(AckRequest {
            delivery_tag: messages[4].delivery_tag,
        })
        .await?;

    // ACK 0 next
    cons.acker
        .send(AckRequest {
            delivery_tag: messages[0].delivery_tag,
        })
        .await?;

    // ACK 2 before 1
    cons.acker
        .send(AckRequest {
            delivery_tag: messages[2].delivery_tag,
        })
        .await?;
    cons.acker
        .send(AckRequest {
            delivery_tag: messages[1].delivery_tag,
        })
        .await?;

    // No redelivery should occur
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert!(cons.messages.try_recv().is_err());
    Ok(())
}

#[tokio::test]
async fn selective_ack_expiry_redelivery() -> anyhow::Result<()> {
    let broker = make_test_broker().await?;

    for i in 0..5 {
        broker.publish("t", format!("v{i}").as_bytes()).await?;
    }

    let mut cons = broker.subscribe("t", "g", make_default_cons_cfg()).await?;

    // Receive all messages 0..4
    let mut msgs = vec![];
    for _ in 0..5 {
        msgs.push(cons.messages.recv().await.context("receiving message")?);
    }

    // ACK everything except offset 2
    for i in [0, 1, 3, 4] {
        cons.acker
            .send(AckRequest {
                delivery_tag: msgs[i].delivery_tag,
            })
            .await?;
    }

    // Let inflight(2) expire
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    // Should re-deliver only offset 2
    let redelivered = cons.messages.recv().await.context("receiving message")?;
    assert_eq!(redelivered.message.offset, 2);

    // ACK it finally
    cons.acker
        .send(AckRequest {
            delivery_tag: redelivered.delivery_tag,
        })
        .await?;

    // Should not redeliver again
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let next = cons.messages.try_recv();
    assert!(next.is_err());
    Ok(())
}

#[tokio::test]
async fn selective_ack_no_wrong_rewind() -> anyhow::Result<()> {
    let broker = make_test_broker().await?;

    for i in 0..3 {
        broker.publish("t", format!("x{i}").as_bytes()).await?;
    }

    let mut cons = broker.subscribe("t", "g", make_default_cons_cfg()).await?;

    let m0 = cons.messages.recv().await.context("receiving message")?;
    let _m1 = cons.messages.recv().await.context("receiving message")?;
    let m2 = cons.messages.recv().await.context("receiving message")?;

    // ACK 2 then 0 (skipping 1)
    cons.acker
        .send(AckRequest {
            delivery_tag: m2.delivery_tag,
        })
        .await?;
    cons.acker
        .send(AckRequest {
            delivery_tag: m0.delivery_tag,
        })
        .await?;

    // No redelivery yet
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert!(cons.messages.try_recv().is_err());

    // Let 1 expire
    tokio::time::sleep(std::time::Duration::from_secs(4)).await;

    // Should redeliver only offset=1
    let redelivered = cons.messages.recv().await.context("receiving message")?;
    assert_eq!(redelivered.message.offset, 1);
    Ok(())
}

#[tokio::test]
async fn batch_basic() -> anyhow::Result<()> {
    let store = make_test_store()?;
    let coord = NoopCoordination {};
    let cfg = BrokerConfig {
        publish_batch_size: 5,
        publish_batch_timeout_ms: 10,
        ..Default::default()
    };
    let metrics = Metrics::new(60 * 60);
    let broker = Arc::new(Broker::try_new(store, coord, metrics.broker(), cfg).await?);

    // Publish 10 messages -> expect two batches
    let mut handles = Vec::new();
    for _ in 0..10 {
        handles.push(tokio::spawn({
            let b = broker.clone();
            async move { b.publish("topic", b"x").await }
        }));
    }

    let mut offsets = Vec::new();
    for h in handles {
        offsets.push(h.await??);
    }

    offsets.sort();
    assert_eq!(offsets, vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);

    // Verify fetch delivers all
    let mut c = broker
        .subscribe("topic", "g", make_default_cons_cfg())
        .await?;
    let mut recv = Vec::new();
    for _ in 0..10 {
        let msg = c.messages.recv().await.context("receiving message")?;
        recv.push(msg.message.offset);
    }
    recv.sort();
    assert_eq!(recv, offsets);
    Ok(())
}

#[tokio::test]
async fn batch_timeout_flushes() -> anyhow::Result<()> {
    let store = make_test_store()?;
    let coord = NoopCoordination {};
    let cfg = BrokerConfig {
        publish_batch_size: 100,
        publish_batch_timeout_ms: 20,
        ..Default::default()
    }; // Large batch size, short timeout

    let metrics = Metrics::new(60 * 60);
    let broker = Arc::new(Broker::try_new(store, coord, metrics.broker(), cfg).await?);

    // Publish 3 messages, waiting briefly so timeout triggers
    let off1 = broker.publish("topic", b"a").await?;
    let off2 = broker.publish("topic", b"b").await?;
    let off3 = broker.publish("topic", b"c").await?;

    assert_eq!(vec![off1, off2, off3], vec![0, 1, 2]);
    Ok(())
}

#[tokio::test]
async fn batch_concurrent_ordering() -> anyhow::Result<()> {
    let store = make_test_store()?;
    let coord = NoopCoordination {};
    let cfg = BrokerConfig {
        publish_batch_size: 10,
        publish_batch_timeout_ms: 50,
        ..Default::default()
    };
    let metrics = Metrics::new(60 * 60);
    let broker = Arc::new(Broker::try_new(store, coord, metrics.broker(), cfg).await?);

    let publish_count = 200;
    let mut tasks = Vec::new();

    for _ in 0..publish_count {
        let b = broker.clone();
        tasks.push(tokio::spawn(async move { b.publish("t", b"m").await }));
    }

    let mut offsets = Vec::new();
    for t in tasks {
        offsets.push(t.await??);
    }

    offsets.sort();
    let expected: Vec<u64> = (0..publish_count).collect();

    assert_eq!(offsets, expected);
    assert_eq!(
        offsets,
        (0..offsets.len() as u64).collect::<Vec<_>>(),
        "phantom or missing offsets detected"
    );
    Ok(())
}

#[tokio::test]
async fn batch_publish_and_consume() -> anyhow::Result<()> {
    let store = make_test_store()?;
    let coord = NoopCoordination {};
    let cfg = BrokerConfig {
        publish_batch_size: 5,
        publish_batch_timeout_ms: 50,
        ..Default::default()
    };

    let metrics = Metrics::new(60 * 60);
    let broker = Arc::new(Broker::try_new(store, coord, metrics.broker(), cfg).await?);

    // Publish 12 messages
    for i in 0..12 {
        broker.publish("topic", format!("x{i}").as_bytes()).await?;
    }

    let mut c = broker
        .subscribe("topic", "g", make_default_cons_cfg())
        .await?;

    let mut seen = Vec::new();
    for _ in 0..12 {
        let msg = c.messages.recv().await.context("receiving message")?;
        seen.push((msg.message.offset, msg.message.payload.clone()));
        c.acker
            .send(AckRequest {
                delivery_tag: msg.delivery_tag,
            })
            .await?;
    }

    seen.sort_by_key(|x| x.0);

    for (i, s) in seen.iter().enumerate().take(12) {
        assert_eq!(s.0, i as u64);
        assert_eq!(s.1, format!("x{i}").as_bytes());
    }
    Ok(())
}

#[tokio::test]
async fn batch_multiple_topics() -> anyhow::Result<()> {
    let store = make_test_store()?;
    let coord = NoopCoordination {};
    let cfg = BrokerConfig {
        publish_batch_size: 4,
        publish_batch_timeout_ms: 20,
        ..Default::default()
    };

    let metrics = Metrics::new(60 * 60);
    let broker = Arc::new(Broker::try_new(store, coord, metrics.broker(), cfg).await?);

    // Publish into two topics interleaved
    let off_a1 = broker.publish("A", b"a1").await?;
    let off_b1 = broker.publish("B", b"b1").await?;
    let off_a2 = broker.publish("A", b"a2").await?;
    let off_b2 = broker.publish("B", b"b2").await?;

    assert_eq!(off_a1, 0);
    assert_eq!(off_a2, 1);
    assert_eq!(off_b1, 0);
    assert_eq!(off_b2, 1);
    Ok(())
}

#[tokio::test]
async fn publish_burst_then_consume_everything() -> anyhow::Result<()> {
    let total = 50_000;
    let max_payload = 512;

    let store = make_test_store()?;
    let coord = NoopCoordination {};
    let cfg = BrokerConfig {
        publish_batch_size: 64,
        publish_batch_timeout_ms: 1,
        ..Default::default()
    };

    let metrics = Metrics::new(60 * 60);
    let broker = Arc::new(Broker::try_new(store, coord, metrics.broker(), cfg).await?);
    let (pubh, mut confirms) = broker.get_publisher("topic").await?;

    // Insert messages with known patterns
    let mut payloads = Vec::new();
    for i in 0..total {
        let size = fastrand::usize(32..max_payload);
        let mut buf = vec![0u8; size];
        fastrand::fill(&mut buf);
        payloads.push((i as u64, buf.clone()));
        pubh.publish(buf).await?;
    }

    // wait until we see total confirms
    let mut n = 0;
    while n < total {
        if let Some(res) = confirms.recv_confirm().await {
            res?; // propagate error
            n += 1;
        } else {
            anyhow::bail!("confirm stream closed early");
        }
    }

    // Now consume them
    let mut c = broker
        .subscribe("topic", "group", make_default_cons_cfg())
        .await?;

    let mut seen = Vec::new();
    for _ in 0..total {
        let msg = c.messages.recv().await.context("receiving message")?;
        seen.push((msg.message.offset, msg.message.payload.clone()));
        c.acker
            .send(AckRequest {
                delivery_tag: msg.delivery_tag,
            })
            .await?;
    }

    // Sort by offset
    seen.sort_by_key(|x| x.0);

    // Validate offsets
    for i in 0..total {
        assert_eq!(seen[i].0, i as u64, "Offset mismatch");
        assert_eq!(seen[i].1, payloads[i].1, "Payload mismatch at offset {}", i,);
    }
    Ok(())
}

#[tokio::test]
async fn concurrent_publish_and_consume() -> anyhow::Result<()> {
    let total = 100_000;

    let store = make_test_store()?;
    let coord = NoopCoordination {};
    let cfg = BrokerConfig {
        publish_batch_size: 64,
        publish_batch_timeout_ms: 1,
        ..Default::default()
    };

    let metrics = Metrics::new(60 * 60);
    let broker = Arc::new(Broker::try_new(store, coord, metrics.broker(), cfg).await?);

    // Start consumer
    let mut consumer = broker
        .subscribe("topic", "g", make_default_cons_cfg())
        .await?;

    // Start publishers
    let mut pub_tasks: Vec<JoinHandle<anyhow::Result<()>>> = Vec::new();
    for _ in 0..4 {
        let b = broker.clone();
        pub_tasks.push(tokio::spawn(async move {
            let (pubh, mut confirms) = b.get_publisher("topic").await?;
            let handle = tokio::spawn(async move {
                for _i in 0..(total / 4) {
                    confirms.recv_confirm().await;
                }
            });
            for i in 0..(total / 4) {
                let mut buf = vec![0u8; 128];
                buf[0..8].copy_from_slice(&(i as u64).to_be_bytes());
                pubh.publish(buf).await?;
            }
            handle.await?;
            Ok(())
        }));
    }

    let res = futures::future::join_all(pub_tasks.into_iter()).await;
    for r in res {
        r??;
    }

    // Collect consumed
    let mut seen = Vec::with_capacity(total);
    for _ in 0..total {
        let msg = consumer
            .messages
            .recv()
            .await
            .context("receiving message")?;
        consumer
            .acker
            .send(AckRequest {
                delivery_tag: msg.delivery_tag,
            })
            .await?;
        seen.push(msg);
    }

    // Check for duplicates
    let mut offsets: Vec<u64> = seen.iter().map(|m| m.message.offset).collect();
    offsets.sort();
    offsets.dedup();
    assert_eq!(offsets.len(), total, "Duplicates detected");
    assert_eq!(
        offsets,
        (0..offsets.len() as u64).collect::<Vec<_>>(),
        "phantom or missing offsets detected"
    );

    // Check consecutive offsets
    for (i, offset) in offsets.into_iter().enumerate().take(total) {
        assert_eq!(offset, i as u64, "Missing or reordered messages");
    }
    Ok(())
}

#[tokio::test]
async fn redelivery_under_load_8k() -> anyhow::Result<()> {
    let total = 8_000;

    redelivery_under_load(total).await?;
    Ok(())
}

#[tokio::test]
async fn redelivery_under_load_2k() -> anyhow::Result<()> {
    let total = 2_000;

    redelivery_under_load(total).await?;
    Ok(())
}

#[tokio::test]
async fn redelivery_under_load_16k() -> anyhow::Result<()> {
    let total = 16_000;

    redelivery_under_load(total).await?;
    Ok(())
}

#[tokio::test]
async fn redelivery_under_load_32k() -> anyhow::Result<()> {
    let total = 32_000;

    redelivery_under_load(total).await?;
    Ok(())
}

#[tokio::test]
async fn redelivery_under_load_64k() -> anyhow::Result<()> {
    let total = 64_000;

    redelivery_under_load(total).await?;
    Ok(())
}

#[tokio::test]
async fn redelivery_under_load_128k() -> anyhow::Result<()> {
    let total = 128_000;

    redelivery_under_load(total).await?;
    Ok(())
}

#[tokio::test]
async fn redelivery_under_load_256k() -> anyhow::Result<()> {
    let total = 256_000;

    redelivery_under_load(total).await?;
    Ok(())
}

async fn redelivery_under_load(total: usize) -> anyhow::Result<()> {
    let store = make_test_store()?;
    let coord = NoopCoordination {};
    let cfg = BrokerConfig {
        publish_batch_timeout_ms: 1,
        inflight_ttl_secs: 5,
        ..Default::default()
    }; // <-- generous TTL

    let metrics = Metrics::new(60 * 60);
    let broker = Arc::new(Broker::try_new(store, coord, metrics.broker(), cfg).await?);

    let (pubh, mut confirms) = broker.get_publisher("t").await?;

    // Publish burst
    for _ in 0..total {
        pubh.publish(b"x".to_vec()).await?;
    }

    // wait until we see total confirms
    let mut n = 0;
    while n < total {
        if let Some(res) = confirms.recv_confirm().await {
            res?; // propagate error
            n += 1;
        } else {
            anyhow::bail!("confirm stream closed early");
        }
    }

    let mut c = broker
        .subscribe(
            "t",
            "g",
            make_default_cons_cfg().with_prefetch_count(total * 100 + 1),
        )
        .await?;

    // FIRST PHASE: receive *all* unique offsets once
    use std::collections::HashSet;
    let mut seen_once = HashSet::new();
    while seen_once.len() < total {
        let msg = c.messages.recv().await.context("receiving message")?;
        seen_once.insert(msg.message.offset);
        // DON'T ACK
    }

    assert_eq!(seen_once.len(), total);

    // Wait for expiry
    tokio::time::sleep(std::time::Duration::from_secs(6)).await;

    // SECOND PHASE: receive at least one more of each
    let mut counts = vec![0usize; total];

    // allow some slack, since we may see dupes
    let max_deliveries = total * 3;
    let mut received = 0;

    while counts.contains(&0) && received < max_deliveries {
        let msg = c.messages.recv().await.context("receiving message")?;
        let idx = msg.message.offset as usize;
        if idx < total {
            counts[idx] += 1;
        }
        // ACK now
        c.acker
            .send(AckRequest {
                delivery_tag: msg.delivery_tag,
            })
            .await?;
        received += 1;
    }

    assert!(
        counts.iter().all(|&c| c >= 1),
        "Some offsets never redelivered at least once"
    );
    Ok(())
}

fn _dupes_with_counts<T>(v: &[T]) -> std::collections::BTreeMap<&T, usize>
where
    T: std::hash::Hash + Eq + Ord,
{
    let mut counts = std::collections::BTreeMap::new();
    for x in v {
        *counts.entry(x).or_insert(0) += 1;
    }
    counts.into_iter().filter(|(_, c)| *c > 1).collect()
}

#[tokio::test]
async fn restart_persists_messages() -> anyhow::Result<()> {
    use fibril_storage::make_rocksdb_store;

    let coord = NoopCoordination {};
    let cfg = BrokerConfig {
        publish_batch_size: 10,
        publish_batch_timeout_ms: 10,
        cleanup_interval_secs: 1,
        ..Default::default()
    };

    // Dedicated DB path for this test
    std::fs::create_dir_all("test_data")?;
    let db_path = format!("test_data/restart_persist_{}", fastrand::u64(..));

    // 1) First broker instance: publish messages
    {
        let store = make_rocksdb_store(&db_path, false)?;
        let metrics = Metrics::new(60 * 60);
        let broker = Broker::try_new(store, coord.clone(), metrics.broker(), cfg.clone()).await?;

        for i in 0..20 {
            broker
                .publish("restart_topic", format!("m{i}").as_bytes())
                .await?;
        }

        // Drop broker (and storage), simulating process exit
        broker.shutdown().await;
        drop(broker);
        std::thread::sleep(std::time::Duration::from_millis(1500));
    }

    // 2) New broker instance on the same path
    {
        let store = make_rocksdb_store(&db_path, false)?;
        let metrics = Metrics::new(60 * 60);
        let broker = Broker::try_new(store, coord.clone(), metrics.broker(), cfg.clone()).await?;

        let mut cons = broker
            .subscribe("restart_topic", "g", make_default_cons_cfg())
            .await?;

        let mut msgs = Vec::new();
        for _ in 0..20 {
            let m = cons.messages.recv().await.context("receiving message")?;
            msgs.push(String::from_utf8(m.message.payload.clone())?);
            cons.acker
                .send(AckRequest {
                    delivery_tag: m.delivery_tag,
                })
                .await?;
        }

        assert_eq!(msgs, (0..20).map(|i| format!("m{i}")).collect::<Vec<_>>());

        // No more messages
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(cons.messages.try_recv().is_err());
    }
    Ok(())
}

#[tokio::test]
async fn restart_persists_ack_state() -> anyhow::Result<()> {
    use fibril_storage::make_rocksdb_store;

    let coord = NoopCoordination {};
    let cfg = BrokerConfig {
        inflight_ttl_secs: 3,
        cleanup_interval_secs: 1,
        publish_batch_size: 10,
        publish_batch_timeout_ms: 10,
        ..BrokerConfig::default()
    };

    std::fs::create_dir_all("test_data")?;
    let db_path = format!("test_data/restart_acks_{}", fastrand::u64(..));

    // 1) First broker: publish and ACK some messages
    {
        let store = make_rocksdb_store(&db_path, false)?;
        let metrics = Metrics::new(60 * 60);
        let broker = Broker::try_new(store, coord.clone(), metrics.broker(), cfg.clone()).await?;
        let (pubh, mut confirms) = broker.get_publisher("restart_ack_topic").await?;

        for i in 0..10 {
            pubh.publish(format!("m{i}").as_bytes().to_vec()).await?;
        }

        for _i in 0..10 {
            confirms.recv_confirm().await;
        }

        let mut cons = broker
            .subscribe("restart_ack_topic", "g", make_default_cons_cfg())
            .await?;

        // ACK first 7 messages
        for _ in 0..7 {
            let m = cons.messages.recv().await.context("receiving message")?;
            cons.acker
                .send(AckRequest {
                    delivery_tag: m.delivery_tag,
                })
                .await?;
        }
        tokio::time::sleep(std::time::Duration::from_millis(1000)).await;

        // Drop broker (simulating crash/restart)
        drop(cons);
        broker.shutdown().await;
        drop(broker);
        std::thread::sleep(std::time::Duration::from_millis(500));
    }

    // 2) Second broker: should only see remaining messages
    {
        // Wait past inflight TTL to ensure no inflight entries remain
        tokio::time::sleep(std::time::Duration::from_secs(4)).await;

        let store = make_rocksdb_store(&db_path, false)?;
        let metrics = Metrics::new(60 * 60);
        let broker = Broker::try_new(store, coord.clone(), metrics.broker(), cfg.clone()).await?;

        let mut cons = broker
            .subscribe("restart_ack_topic", "g", make_default_cons_cfg())
            .await?;

        let mut seen = Vec::new();
        // Expect only 3 messages left
        for _ in 0..3 {
            let m = cons.messages.recv().await.context("receiving message")?;
            seen.push(String::from_utf8(m.message.payload.clone())?);
            cons.acker
                .send(AckRequest {
                    delivery_tag: m.delivery_tag,
                })
                .await?;
        }

        // No more messages should arrive
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert!(cons.messages.try_recv().is_err());

        assert_eq!(
            seen,
            vec!["m7".to_string(), "m8".to_string(), "m9".to_string()]
        );
    }
    Ok(())
}

#[tokio::test]
async fn restart_redelivery_across_restart() -> anyhow::Result<()> {
    use fibril_storage::make_rocksdb_store;

    let coord = NoopCoordination {};
    let cfg = BrokerConfig {
        publish_batch_size: 16,
        publish_batch_timeout_ms: 3,
        inflight_ttl_secs: 1,
        cleanup_interval_secs: 7,
        ..Default::default()
    };

    std::fs::create_dir_all("test_data")?;
    let db_path = format!("test_data/restart_redel_{}", fastrand::u64(..));

    let offset: Offset;
    let topic: Topic = String::from("rr_topic");

    // 1) First broker: publish + first delivery (no ACK)
    {
        let store = make_rocksdb_store(&db_path, false)?;
        let metrics = Metrics::new(60 * 60);
        let broker = Broker::try_new(store, coord.clone(), metrics.broker(), cfg.clone()).await?;

        let (pubh, mut confirms) = broker.get_publisher(&topic).await?;
        pubh.publish(b"hello".to_vec()).await?;

        confirms.recv_confirm().await;

        let mut cons = broker
            .subscribe(&topic, "g", make_default_cons_cfg())
            .await?;
        let m1 = cons.messages.recv().await.context("receiving message")?;
        assert_eq!(m1.message.payload, b"hello");
        offset = m1.message.offset;

        // Do not ACK
        drop(cons);
        drop(confirms);
        broker.shutdown().await;
        drop(broker);
    }

    // Wait past TTL so the inflight entry is expired in storage
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    // 2) Second broker: same DB, new worker, must redeliver
    {
        let store = make_rocksdb_store(&db_path, false)?;
        let metrics = Metrics::new(60 * 60);
        let broker =
            Arc::new(Broker::try_new(store, coord.clone(), metrics.broker(), cfg.clone()).await?);

        let mut cons = broker
            .subscribe(&topic, "g", make_default_cons_cfg())
            .await?;

        let redelivered = cons.messages.recv().await.context("receiving message")?;
        assert_eq!(redelivered.message.offset, offset);
        assert_eq!(redelivered.message.payload, b"hello");

        cons.acker
            .send(AckRequest {
                delivery_tag: redelivered.delivery_tag,
            })
            .await?;
        drop(cons);
        broker.shutdown().await;
        drop(broker);
    }
    Ok(())
}

#[tokio::test]
async fn work_queue_fair_distribution() -> anyhow::Result<()> {
    let broker = make_test_broker().await?;

    let total = 6000;

    let (pubh, mut confirms) = broker.get_publisher("jobs_fair").await?;

    // Publish work
    for i in 0..total {
        pubh.publish(format!("job-{i}").as_bytes().to_vec()).await?;
    }

    tokio::spawn(async move {
        for _i in 0..total {
            confirms.recv_confirm().await;
        }
    });

    let mut c1 = broker
        .subscribe("jobs_fair", "workers", make_default_cons_cfg())
        .await?;
    let mut c2 = broker
        .subscribe("jobs_fair", "workers", make_default_cons_cfg())
        .await?;
    let mut c3 = broker
        .subscribe("jobs_fair", "workers", make_default_cons_cfg())
        .await?;

    let mut counts = [0usize; 3];

    for _ in 0..total {
        tokio::select! {
            Some(msg) = c1.messages.recv() => {
                counts[0] += 1;
                c1.acker.send(AckRequest { delivery_tag: msg.delivery_tag }).await?;
            }
            Some(msg) = c2.messages.recv() => {
                counts[1] += 1;
                c2.acker.send(AckRequest { delivery_tag: msg.delivery_tag }).await?;
            }
            Some(msg) = c3.messages.recv() => {
                counts[2] += 1;
                c3.acker.send(AckRequest { delivery_tag: msg.delivery_tag }).await?;
            }
            else => anyhow::bail!("all consumers closed unexpectedly"),
        }
    }

    let sum: usize = counts.iter().sum();
    assert_eq!(sum, total);

    // Very loose fairness bounds: each should get at least 10% of the work
    for (i, c) in counts.iter().enumerate() {
        assert!(
            *c >= total / 10,
            "consumer {} got too little work: {} of {}",
            i,
            c,
            total
        );
    }
    Ok(())
}

#[tokio::test]
async fn multi_topic_multi_group_isolation() -> anyhow::Result<()> {
    let broker = make_test_broker().await?;

    // Topic A: 100 messages, group GA
    for i in 0..100 {
        broker.publish("A", format!("a-{i}").as_bytes()).await?;
    }

    // Topic B: 60 messages, groups GB1 and GB2
    for i in 0..60 {
        broker.publish("B", format!("b-{i}").as_bytes()).await?;
    }

    let mut a_ga = broker.subscribe("A", "GA", make_default_cons_cfg()).await?;

    let mut b_gb1 = broker
        .subscribe("B", "GB1", make_default_cons_cfg())
        .await?;
    let mut b_gb2 = broker
        .subscribe("B", "GB2", make_default_cons_cfg())
        .await?;

    // Collect A/GA
    let mut a_seen = Vec::new();
    for _ in 0..100 {
        let m = a_ga.messages.recv().await.context("receiving message")?;
        a_seen.push(String::from_utf8(m.message.payload.clone())?);
        a_ga.acker
            .send(AckRequest {
                delivery_tag: m.delivery_tag,
            })
            .await?;
    }

    // Collect B/GB1 and B/GB2 (fanout: both see all messages)
    let mut b1_seen = Vec::new();
    let mut b2_seen = Vec::new();

    for _ in 0..60 {
        let m1 = b_gb1.messages.recv().await.context("receiving message")?;
        let m2 = b_gb2.messages.recv().await.context("receiving message")?;

        b1_seen.push(String::from_utf8(m1.message.payload.clone())?);
        b2_seen.push(String::from_utf8(m2.message.payload.clone())?);

        b_gb1
            .acker
            .send(AckRequest {
                delivery_tag: m1.delivery_tag,
            })
            .await?;
        b_gb2
            .acker
            .send(AckRequest {
                delivery_tag: m2.delivery_tag,
            })
            .await?;
    }

    // Assertions
    assert_eq!(
        a_seen,
        (0..100).map(|i| format!("a-{i}")).collect::<Vec<_>>()
    );
    assert_eq!(
        b1_seen,
        (0..60).map(|i| format!("b-{i}")).collect::<Vec<_>>()
    );
    assert_eq!(b1_seen, b2_seen); // both groups see same B stream

    // No topic bleed
    assert!(a_seen.iter().all(|s| s.starts_with("a-")));
    assert!(b1_seen.iter().all(|s| s.starts_with("b-")));
    Ok(())
}

#[tokio::test]
async fn randomized_publish_consume_fuzz_delivery_tags() -> anyhow::Result<()> {
    let store = make_test_store()?;
    let coord = NoopCoordination {};
    let cfg = BrokerConfig {
        publish_batch_size: 32,
        publish_batch_timeout_ms: 5,
        inflight_ttl_secs: 2,
        ..Default::default()
    };

    let metrics = Metrics::new(60 * 60);
    let broker = Arc::new(Broker::try_new(store, coord, metrics.broker(), cfg).await?);

    let mut cons = broker
        .subscribe(
            "fuzz_topic",
            "g",
            make_default_cons_cfg().with_prefetch_count(2048),
        )
        .await?;

    let total = 10_000;

    // Publisher task
    let (b_pub, mut confirms) = broker.get_publisher("fuzz_topic").await?;
    let pub_task: JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
        for i in 0..total {
            let action = fastrand::u8(..100);

            if action < 80 {
                // publish
                let payload = format!("m-{i}").into_bytes();
                b_pub.publish(payload).await?;
            } else {
                // short pause to mix timing
                tokio::time::sleep(std::time::Duration::from_micros(100)).await;
            }
        }
        Ok(())
    });
    let conf_task: JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
        while (confirms.recv_confirm().await).is_some() {}
        Ok(())
    });

    // Consumer/ACK behavior with some random drops
    let mut received = Vec::new();
    let mut acked = Vec::new();

    while received.len() < total {
        if let Some(msg) = cons.messages.recv().await {
            let delivery_tag = msg.delivery_tag;
            let payload = msg.message.payload.clone();
            received.push((delivery_tag, payload.clone()));

            let r = fastrand::u8(..100);
            if r < 70 {
                // ACK most of the time
                cons.acker
                    .send(AckRequest {
                        delivery_tag: msg.delivery_tag,
                    })
                    .await?;
                acked.push(delivery_tag);
            } else {
                // Let some expire for redelivery
            }
        }
    }

    pub_task.await??;
    conf_task.await??;

    // Wait for redeliveries to settle
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    // Drain any last messages
    while let Ok(msg) = cons.messages.try_recv() {
        received.push((msg.delivery_tag, msg.message.payload.clone()));
        cons.acker
            .send(AckRequest {
                delivery_tag: msg.delivery_tag,
            })
            .await?;
    }

    // Invariants:
    // - No gaps in delivery tags from 0..max_delivery_tag
    // - Every delivery tag that ever existed appears at least once in `received`
    let mut delivery_tags: Vec<u64> = received.iter().map(|(o, _)| *o).collect();
    delivery_tags.sort();
    delivery_tags.dedup();

    let max_delivery_tag = *delivery_tags
        .last()
        .ok_or(anyhow::Error::msg("delivery_tags empty"))?;
    assert_eq!(delivery_tags, (0..=max_delivery_tag).collect::<Vec<_>>());
    assert_eq!(
        delivery_tags,
        (0..delivery_tags.len() as u64).collect::<Vec<_>>(),
        "phantom or missing delivery tags detected"
    );
    Ok(())
}

#[tokio::test]
async fn randomized_publish_consume_fuzz_offsets() -> anyhow::Result<()> {
    let store = make_test_store()?;
    let coord = NoopCoordination {};
    let cfg = BrokerConfig {
        publish_batch_size: 32,
        publish_batch_timeout_ms: 5,
        inflight_ttl_secs: 2,
        ..Default::default()
    };

    let metrics = Metrics::new(60 * 60);
    let broker = Arc::new(Broker::try_new(store, coord, metrics.broker(), cfg).await?);

    let mut cons = broker
        .subscribe(
            "fuzz_topic",
            "g",
            make_default_cons_cfg().with_prefetch_count(2048),
        )
        .await?;

    let total = 10_000;

    // Publisher task
    let (b_pub, mut confirms) = broker.get_publisher("fuzz_topic").await?;
    let pub_task: JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
        for i in 0..total {
            let action = fastrand::u8(..100);

            if action < 80 {
                // publish
                let payload = format!("m-{i}").into_bytes();
                b_pub.publish(payload).await?;
            } else {
                // short pause to mix timing
                tokio::time::sleep(std::time::Duration::from_micros(100)).await;
            }
        }
        Ok(())
    });
    let conf_task = tokio::spawn(async move { while confirms.recv_confirm().await.is_some() {} });

    // Consumer/ACK behavior with some random drops
    let mut received = Vec::new();
    let mut acked = Vec::new();

    while received.len() < total {
        if let Some(msg) = cons.messages.recv().await {
            let offset = msg.message.offset;
            let payload = msg.message.payload.clone();
            received.push((offset, payload.clone()));

            let r = fastrand::u8(..100);
            if r < 70 {
                // ACK most of the time
                cons.acker
                    .send(AckRequest {
                        delivery_tag: msg.delivery_tag,
                    })
                    .await?;
                acked.push(offset);
            } else {
                // Let some expire for redelivery
            }
        }
    }

    pub_task.await??;
    conf_task.await?;

    // Wait for redeliveries to settle
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    // Drain any last messages
    while let Ok(msg) = cons.messages.try_recv() {
        received.push((msg.message.offset, msg.message.payload.clone()));
        cons.acker
            .send(AckRequest {
                delivery_tag: msg.delivery_tag,
            })
            .await?;
    }

    // Invariants:
    // - No gaps in offsets from 0..max_offset
    // - Every offset that ever existed appears at least once in `received`
    let mut offsets: Vec<u64> = received.iter().map(|(o, _)| *o).collect();
    offsets.sort();
    offsets.dedup();

    let max_offset = *offsets.last().ok_or(anyhow::Error::msg("offsets empty"))?;
    assert_eq!(offsets, (0..=max_offset).collect::<Vec<_>>());
    assert_eq!(
        offsets,
        (0..offsets.len() as u64).collect::<Vec<_>>(),
        "phantom or missing offsets detected"
    );
    Ok(())
}

#[tokio::test]
async fn storage_inflight_implies_message_exists() -> anyhow::Result<()> {
    let store = make_test_store()?;

    let topic = "t".to_string();
    let group = "g".to_string();
    let partition = 0;

    let off = store.append(&topic, partition, b"x").await?;
    store
        .mark_inflight(&topic, partition, &group, off, unix_millis() + 100000)
        .await?;

    // Run cleanup aggressively
    store.cleanup_topic(&topic, partition).await?;

    // Invariant: if inflight exists, message must exist OR inflight must be gone
    let inflight = store
        .is_inflight_or_acked(&topic, partition, &group, off)
        .await?;

    if inflight {
        let res = store.fetch_by_offset(&topic, partition, off).await;
        assert!(
            res.is_ok(),
            "inflight exists but message missing: {:?}",
            res
        );
    }
    Ok(())
}

#[tokio::test]
async fn cursor_never_moves_backwards() -> anyhow::Result<()> {
    let broker = make_test_broker().await?;

    for i in 0..10 {
        broker.publish("t", format!("m{i}").as_bytes()).await?;
    }

    let mut c = broker.subscribe("t", "g", make_default_cons_cfg()).await?;

    let mut last = None;

    for _ in 0..10 {
        let m = c.messages.recv().await.context("receiving message")?;
        if let Some(prev) = last {
            assert!(m.message.offset > prev, "cursor went backwards");
        }
        last = Some(m.message.offset);
        c.acker
            .send(AckRequest {
                delivery_tag: m.delivery_tag,
            })
            .await?;
    }
    Ok(())
}

#[tokio::test]
async fn crash_after_send_before_inflight_causes_redelivery() -> anyhow::Result<()> {
    let coord = NoopCoordination {};
    let cfg = BrokerConfig {
        inflight_ttl_secs: 1,
        cleanup_interval_secs: 5,
        ..Default::default()
    };

    let db_path = format!("test_data/crash_inflight_{}", fastrand::u64(..));
    let offset;

    {
        let store = make_rocksdb_store(&db_path, false)?;
        let metrics = Metrics::new(60 * 60);
        let broker = Broker::try_new(store, coord.clone(), metrics.broker(), cfg.clone()).await?;
        let (pubh, mut confirms) = broker.get_publisher("t").await?;
        pubh.publish(b"x".to_vec()).await?;
        confirms.recv_confirm().await;
        let mut c = broker.subscribe("t", "g", make_default_cons_cfg()).await?;

        let m = c.messages.recv().await.context("receiving message")?;
        offset = m.message.offset;

        // NO ACK, NO TIME for inflight batch flush
        drop(c);
        broker.shutdown().await;
    }

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    {
        let store = make_rocksdb_store(&db_path, false)?;

        let metrics = Metrics::new(60 * 60);
        let broker = Arc::new(Broker::try_new(store, coord, metrics.broker(), cfg).await?);
        let mut c = broker.subscribe("t", "g", make_default_cons_cfg()).await?;

        let redelivered = c.messages.recv().await.context("receiving message")?;
        assert_eq!(redelivered.message.offset, offset);
    }
    Ok(())
}

#[tokio::test]
async fn ack_before_delivery_is_ignored() -> anyhow::Result<()> {
    let broker = make_test_broker().await?;

    broker.publish("t", b"x").await?;

    let mut c = broker.subscribe("t", "g", make_default_cons_cfg()).await?;

    // ACK offset 0 before receiving it
    c.acker.send(AckRequest { delivery_tag: 0 }).await?;

    // Must still be delivered
    let m = c.messages.recv().await.context("receiving message")?;
    assert_eq!(m.message.offset, 0);

    c.acker
        .send(AckRequest {
            delivery_tag: m.delivery_tag,
        })
        .await?;
    Ok(())
}

#[tokio::test]
async fn redelivery_does_not_advance_cursor() -> anyhow::Result<()> {
    let broker = make_test_broker().await?;

    broker.publish("t", b"a").await?;
    broker.publish("t", b"b").await?;
    broker.publish("t", b"c").await?;
    broker.publish("t", b"d").await?;

    let mut c = broker
        .subscribe("t", "g", make_default_cons_cfg().with_prefetch_count(1))
        .await?;

    let m0 = c.messages.recv().await.context("receiving message")?; // don't ack

    tokio::time::sleep(std::time::Duration::from_secs(4)).await;

    let redelivered = c.messages.recv().await.context("receiving message")?;
    assert_eq!(redelivered.message.offset, m0.message.offset);

    // ACK now
    c.acker
        .send(AckRequest {
            delivery_tag: redelivered.delivery_tag,
        })
        .await?;

    // Next message must be offset 1
    let m1 = c.messages.recv().await.context("receiving message")?;
    assert_eq!(m1.message.offset, 1);
    Ok(())
}

#[tokio::test]
async fn prefetch_limits_inflight() -> anyhow::Result<()> {
    let broker = make_test_broker().await?;

    // publish 3 messages
    for b in [b"a", b"b", b"c", b"d", b"e", b"f", b"g"] {
        broker.publish("t", b).await?;
    }

    let mut c = broker
        .subscribe("t", "g", make_default_cons_cfg().with_prefetch_count(1))
        .await?; // prefetch = 1

    // receive first
    let m0 = c.recv().await.context("receiving message")?;
    assert_eq!(m0.message.offset, 0);

    // should NOT receive second until ack or expiry
    let mut extra = 0;
    let start = tokio::time::Instant::now();

    loop {
        let res = tokio::time::timeout(std::time::Duration::from_millis(50), c.recv()).await;

        match res {
            Ok(Some(m)) => {
                extra += 1;
                tracing::info!("extra recv offset={}", m.message.offset);
            }
            _ => break,
        }

        if start.elapsed().as_millis() > 300 {
            break;
        }
    }

    assert_eq!(extra, 0, "received {extra} extra messages beyond prefetch");
    Ok(())
}

#[tokio::test]
async fn prefetch_releases_on_ack() -> anyhow::Result<()> {
    let broker = make_test_broker().await?;

    broker.publish("t", b"a").await?;
    broker.publish("t", b"b").await?;
    broker.publish("t", b"c").await?;
    broker.publish("t", b"d").await?;

    let mut c = broker
        .subscribe("t", "g", make_default_cons_cfg().with_prefetch_count(1))
        .await?;

    let m0 = c.recv().await.context("receiving message")?;
    assert_eq!(m0.message.offset, 0);

    c.ack(AckRequest {
        delivery_tag: m0.delivery_tag,
    })
    .await?;

    let m1 = c.recv().await.context("receiving message")?;
    assert_eq!(m1.message.offset, 1);
    Ok(())
}

#[tokio::test]
async fn prefetch_releases_on_expiry() -> anyhow::Result<()> {
    let broker = make_test_broker_with_cfg(BrokerConfig {
        inflight_ttl_secs: 1,
        ..Default::default()
    })
    .await?;

    let (publisher, _confirmer) = broker.get_publisher("t").await?;

    publisher.publish(b"a".to_vec()).await?;
    publisher.publish(b"b".to_vec()).await?;
    publisher.publish(b"c".to_vec()).await?;
    publisher.publish(b"d".to_vec()).await?;

    let mut c = broker
        .subscribe("t", "g", make_default_cons_cfg().with_prefetch_count(1))
        .await?;

    let m0 = c.recv().await.context("receiving message")?;
    assert_eq!(m0.message.offset, 0);

    // don't ack, wait for expiry
    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;

    // redelivery should come, not offset 1
    let redelivered = c.recv().await.context("receiving message")?;
    assert_eq!(redelivered.message.offset, 0);

    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    // ack redelivery
    c.ack(AckRequest {
        delivery_tag: redelivered.delivery_tag,
    })
    .await?;

    // now offset 1
    let m1 = c.recv().await.context("receiving message")?;
    assert_eq!(m1.message.offset, 1);
    Ok(())
}

#[tokio::test]
async fn offsets_are_never_reused_after_cleanup() -> anyhow::Result<()> {
    let broker = make_test_broker().await?;

    let _o0 = broker.publish("t", b"a").await?;
    let o1 = broker.publish("t", b"b").await?;

    let mut c = broker.subscribe("t", "g", make_default_cons_cfg()).await?;

    let m0 = c.recv().await.context("receiving message")?;
    c.ack(AckRequest {
        delivery_tag: m0.delivery_tag,
    })
    .await?;

    let m1 = c.recv().await.context("receiving message")?;
    c.ack(AckRequest {
        delivery_tag: m1.delivery_tag,
    })
    .await?;

    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    // force cleanup
    broker.forced_cleanup(&"t".into(), 0).await?;

    let o2 = broker.publish("t", b"c").await?;

    assert!(o2 > o1, "offset reused after cleanup");
    Ok(())
}

#[tokio::test]
async fn redelivery_has_priority_over_fresh() -> anyhow::Result<()> {
    let broker = make_test_broker_with_cfg(BrokerConfig {
        inflight_ttl_secs: 1,
        ..Default::default()
    })
    .await?;

    for ch in [b"a", b"b", b"c"] {
        broker.publish("t", ch).await?;
    }

    let mut c = broker
        .subscribe("t", "g", make_default_cons_cfg().with_prefetch_count(2))
        .await?;

    let m0 = c.recv().await.context("receiving message")?; // don't ack 0
    let _m1 = c.recv().await.context("receiving message")?; // allow prefetch to pull more

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // Next delivery must be a redelivery of 0 (priority), not fresh 2
    let next = c.recv().await.context("receiving message")?;
    assert_eq!(next.message.offset, m0.message.offset);
    Ok(())
}

#[tokio::test]
async fn inflight_capacity_never_exceeds_prefetch() -> anyhow::Result<()> {
    let broker = make_test_broker().await?;
    let mut c = broker
        .subscribe("t", "g", make_default_cons_cfg().with_prefetch_count(1))
        .await?;

    broker.publish("t", b"a").await?;
    broker.publish("t", b"b").await?;

    let m0 = c.recv().await.context("receiving message")?;
    c.ack(AckRequest {
        delivery_tag: m0.delivery_tag,
    })
    .await?;

    // If capacity is double released, this would allow receiving both
    let m1 = c.recv().await.context("receiving message")?;
    assert_eq!(m1.message.offset, 1);

    // No extra messages allowed
    assert!(c.messages.try_recv().is_err());
    Ok(())
}

#[tokio::test]
async fn ack_spam_does_not_increase_capacity() -> anyhow::Result<()> {
    let broker = make_test_broker().await?;

    for _ in 0..10 {
        broker.publish("t", b"x").await?;
    }

    let mut c = broker
        .subscribe("t", "g", make_default_cons_cfg().with_prefetch_count(1))
        .await?;

    // Receive first message
    let m = c.recv().await.context("receiving message")?;

    // Spam ACKs for the same delivery tag
    for _ in 0..100 {
        c.ack(AckRequest {
            delivery_tag: m.delivery_tag,
        })
        .await?;
    }

    // Allow scheduler to run
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // At most ONE new message must arrive
    let m2 = c.recv().await.context("receiving message")?;
    assert_eq!(m2.message.offset, 1);

    // And nothing more
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(c.messages.try_recv().is_err());

    Ok(())
}

#[tokio::test]
async fn ack_before_delivery_cannot_free_capacity() -> anyhow::Result<()> {
    let broker = make_test_broker_with_cfg(BrokerConfig {
        ack_batch_size: 8,
        ack_batch_timeout_ms: 50,
        inflight_ttl_secs: 2,
        ..Default::default()
    })
    .await?;

    let (pubh, mut confirms) = broker.get_publisher("t").await?;
    let handle = tokio::spawn(async move {
        for _ in 0..5 {
            confirms
                .recv_confirm()
                .await
                .context("receiving message")??;
        }

        Ok::<_, anyhow::Error>(())
    });
    for i in 0..5 {
        pubh.publish(format!("m{i}").as_bytes().to_vec()).await?;
    }

    handle.await??;

    let mut c = broker
        .subscribe("t", "g", make_default_cons_cfg().with_prefetch_count(1))
        .await?;

    // ACK offsets that haven't been delivered yet
    for off in 0..5 {
        c.ack(AckRequest { delivery_tag: off }).await?;
    }

    // Now receive real delivery
    let m0 = c.recv().await.context("receiving message")?;
    assert_eq!(m0.message.offset, 0);

    // Do NOT receive offset 1 yet
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert!(c.messages.try_recv().is_err());

    // ACK real delivery
    c.ack(AckRequest {
        delivery_tag: m0.delivery_tag,
    })
    .await?;

    // Now offset 1 is allowed
    let m1 = c.recv().await.context("receiving message")?;
    assert_eq!(m1.message.offset, 1);

    Ok(())
}

#[tokio::test]
async fn expiry_and_ack_race_never_double_frees() -> anyhow::Result<()> {
    let broker = make_test_broker_with_cfg(BrokerConfig {
        inflight_ttl_secs: 1,
        ..Default::default()
    })
    .await?;

    broker.publish("t", b"x").await?;

    let mut c = broker
        .subscribe("t", "g", make_default_cons_cfg().with_prefetch_count(1))
        .await?;

    let m = c.recv().await.context("receiving message")?;

    // ACK *right* around expiry
    tokio::time::sleep(std::time::Duration::from_millis(950)).await;
    c.ack(AckRequest {
        delivery_tag: m.delivery_tag,
    })
    .await?;

    // Wait past expiry window
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Must NOT redeliver AND must not allow extra capacity
    assert!(c.messages.try_recv().is_err());

    Ok(())
}
