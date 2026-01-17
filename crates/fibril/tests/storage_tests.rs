use fibril_storage::*;

fn make_test_store() -> anyhow::Result<Box<dyn Storage>> {
    // make testdata dir
    std::fs::create_dir_all("test_data")?;
    // make random temp filename to avoid conflicts
    let filename = format!("test_data/{}", fastrand::u64(..));
    Ok(Box::new(make_rocksdb_store(&filename, false)?))
}

#[tokio::test]
async fn append_and_fetch() -> anyhow::Result<()> {
    let store = make_test_store()?; // returns dyn Storage boxed

    let topic = "t1".to_string();
    let group = "g1".to_string();

    let o1 = store.append(&topic, 0, b"hello").await?;
    let o2 = store.append(&topic, 0, b"world").await?;

    assert_eq!(o1, 0);
    assert_eq!(o2, 1);

    let msgs = store.fetch_available(&topic, 0, &group, 0, 10).await?;
    assert_eq!(msgs.len(), 2);
    assert_eq!(msgs[0].message.payload, b"hello");
    assert_eq!(msgs[1].message.payload, b"world");
    Ok(())
}

#[tokio::test]
async fn inflight_and_ack() -> anyhow::Result<()> {
    let store = make_test_store()?;

    let topic = "t".to_string();
    let group = "g".to_string();

    store.append(&topic, 0, b"a").await?;
    store.append(&topic, 0, b"b").await?;

    let msgs = store.fetch_available(&topic, 0, &group, 0, 10).await?;
    assert_eq!(msgs.len(), 2);

    store
        .mark_inflight(&topic, 0, &group, msgs[0].message.offset, 999999)
        .await?;
    store.ack(&topic, 0, &group, msgs[0].message.offset).await?;

    let msgs2 = store.fetch_available(&topic, 0, &group, 0, 10).await?;
    assert_eq!(msgs2.len(), 1);
    Ok(())
}

#[tokio::test]
async fn redelivery() -> anyhow::Result<()> {
    let store = make_test_store()?;
    let topic = "t".to_string();
    let group = "g".to_string();

    store.append(&topic, 0, b"x").await?;

    let msgs = store.fetch_available(&topic, 0, &group, 0, 10).await?;
    let off = msgs[0].message.offset;

    store.mark_inflight(&topic, 0, &group, off, 0).await?; // already expired

    let expired = store.list_expired(100).await?;
    assert_eq!(expired.len(), 1);
    Ok(())
}

#[tokio::test]
async fn out_of_order_acks() -> anyhow::Result<()> {
    let store = make_test_store()?;

    let topic = "t".to_string();
    let group = "g".to_string();

    for i in 0..5 {
        store
            .append(&topic, 0, format!("msg-{i}").as_bytes())
            .await?;
    }

    let msgs = store.fetch_available(&topic, 0, &group, 0, 10).await?;
    assert_eq!(msgs.len(), 5);

    // ACK messages 2 and 4 first
    store.mark_inflight(&topic, 0, &group, 2, 1_000_000).await?;
    store.ack(&topic, 0, &group, 2).await?;

    store.mark_inflight(&topic, 0, &group, 4, 1_000_000).await?;
    store.ack(&topic, 0, &group, 4).await?;

    let msgs2 = store.fetch_available(&topic, 0, &group, 0, 10).await?;

    // Offsets 0,1,3 are remaining
    let offsets: Vec<_> = msgs2.iter().map(|m| m.message.offset).collect();
    assert_eq!(offsets, vec![0, 1, 3]);
    Ok(())
}

#[tokio::test]
async fn redelivery_after_expiration() -> anyhow::Result<()> {
    let store = make_test_store()?;
    let topic = "redel".to_string();
    let group = "g".to_string();

    let off = store.append(&topic, 0, b"a").await?;

    let msgs = store.fetch_available(&topic, 0, &group, 0, 10).await?;
    assert_eq!(msgs.len(), 1);

    // Mark inflight with a short deadline
    store.mark_inflight(&topic, 0, &group, off, 10).await?;

    // Expired now
    let expired = store.list_expired(20).await?;
    assert_eq!(expired.len(), 1);
    assert_eq!(expired[0].message.offset, off);

    // After expiration, fetch_available should NOT see inflight entry
    // (because redelivery clears it — your code will need to)
    Ok(())
}

#[tokio::test]
async fn multiple_groups_independent_offsets() -> anyhow::Result<()> {
    let store = make_test_store()?;
    let topic = "multi".to_string();
    let g1 = "g1".to_string();
    let g2 = "g2".to_string();

    for i in 0..3 {
        store
            .append(&topic, 0, format!("msg-{i}").as_bytes())
            .await?;
    }

    // Group 1 fetches and acks msg 0
    let msgs = store.fetch_available(&topic, 0, &g1, 0, 10).await?;
    assert_eq!(msgs.len(), 3);
    store.mark_inflight(&topic, 0, &g1, 0, 999).await?;
    store.ack(&topic, 0, &g1, 0).await?;

    // Group 2 should still see all messages
    let msgs2 = store.fetch_available(&topic, 0, &g2, 0, 10).await?;
    assert_eq!(msgs2.len(), 3);
    Ok(())
}

#[tokio::test]
async fn fetch_max_limit_respected() -> anyhow::Result<()> {
    let store = make_test_store()?;
    let topic = "limit".to_string();
    let group = "g".to_string();

    for i in 0..10 {
        store
            .append(&topic, 0, format!("msg-{i}").as_bytes())
            .await?;
    }

    let msgs = store.fetch_available(&topic, 0, &group, 0, 3).await?;
    assert_eq!(msgs.len(), 3);
    Ok(())
}

#[tokio::test]
async fn out_of_order_ack_behavior() -> anyhow::Result<()> {
    let store = make_test_store()?;
    let topic = "topic1".to_string();
    let group = "group1".to_string();

    // Write 3 messages: 0,1,2
    for i in 0..3 {
        store.append(&topic, 0, format!("m{i}").as_bytes()).await?;
    }

    let msgs = store.fetch_available(&topic, 0, &group, 0, 10).await?;
    assert_eq!(msgs.len(), 3);

    // ACK 2, skip 1
    store.mark_inflight(&topic, 0, &group, 2, 999).await?;
    store.ack(&topic, 0, &group, 2).await?;

    // ACK 0
    store.mark_inflight(&topic, 0, &group, 0, 999).await?;
    store.ack(&topic, 0, &group, 0).await?;

    let msgs2 = store.fetch_available(&topic, 0, &group, 0, 10).await?;
    assert_eq!(msgs2.len(), 1);
    assert_eq!(msgs2[0].message.offset, 1);
    Ok(())
}

#[tokio::test]
async fn inflight_messages_not_fetched() -> anyhow::Result<()> {
    let store = make_test_store()?;
    let topic = "t2".to_string();
    let group = "g".to_string();

    store.append(&topic, 0, b"a").await?; // offset 0
    store.append(&topic, 0, b"b").await?; // offset 1

    store.mark_inflight(&topic, 0, &group, 0, 1_000_000).await?;

    let msgs = store.fetch_available(&topic, 0, &group, 0, 10).await?;
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].message.offset, 1);
    Ok(())
}

#[tokio::test]
async fn expired_messages_are_redelivered() -> anyhow::Result<()> {
    let store = make_test_store()?;
    let topic = "t3".to_string();
    let group = "g".to_string();

    let off = store.append(&topic, 0, b"x").await?;

    // Fetch and mark inflight
    let msgs = store.fetch_available(&topic, 0, &group, 0, 1).await?;
    assert_eq!(msgs[0].message.offset, off);

    store.mark_inflight(&topic, 0, &group, off, 10).await?; // expired at ts>10

    // Should be expired
    let expired = store.list_expired(11).await?;
    assert_eq!(expired.len(), 1);

    // After expiration, message shouldn't be "inflight" anymore
    // (you will add this logic inside broker layer or storage)
    Ok(())
}

#[tokio::test]
async fn consumer_groups_are_isolated() -> anyhow::Result<()> {
    let store = make_test_store()?;
    let t = "topicX".to_string();
    let g1 = "G1".to_string();
    let g2 = "G2".to_string();

    for i in 0..3 {
        store.append(&t, 0, format!("v{i}").as_bytes()).await?;
    }

    // Group 1 acks message 0
    store.mark_inflight(&t, 0, &g1, 0, 999).await?;
    store.ack(&t, 0, &g1, 0).await?;

    // Group 2 should still see all 3
    let msgs_g2 = store.fetch_available(&t, 0, &g2, 0, 10).await?;
    assert_eq!(msgs_g2.len(), 3);
    Ok(())
}

#[tokio::test]
async fn prefix_isolation() -> anyhow::Result<()> {
    let store = make_test_store()?;

    let t1 = "A".to_string();
    let t2 = "B".to_string();
    let g = "G".to_string();

    store.append(&t1, 0, b"x").await?;
    store.append(&t2, 0, b"y").await?;

    let msgs = store.fetch_available(&t1, 0, &g, 0, 10).await?;

    // Must not return messages for topic B
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].message.payload, b"x");
    Ok(())
}

#[tokio::test]
async fn large_offset_ranges() -> anyhow::Result<()> {
    let store = make_test_store()?;
    let topic = "big".to_string();
    let group = "g".to_string();

    for i in 0..2000 {
        store.append(&topic, 0, format!("{i}").as_bytes()).await?;
    }

    let msgs = store.fetch_available(&topic, 0, &group, 1500, 100).await?;

    assert_eq!(msgs[0].message.offset, 1500);
    assert_eq!(msgs.len(), 100);
    Ok(())
}

#[tokio::test]
async fn no_double_delivery_after_ack() -> anyhow::Result<()> {
    let store = make_test_store()?;
    let topic = "nodup".to_string();
    let group = "g".to_string();

    store.append(&topic, 0, b"a").await?;
    store.append(&topic, 0, b"b").await?;

    let _msgs = store.fetch_available(&topic, 0, &group, 0, 10).await?;

    // ACK the first
    store.mark_inflight(&topic, 0, &group, 0, 999).await?;
    store.ack(&topic, 0, &group, 0).await?;

    let msgs2 = store.fetch_available(&topic, 0, &group, 0, 10).await?;

    assert_eq!(msgs2.len(), 1);
    assert_eq!(msgs2[0].message.offset, 1);
    Ok(())
}

#[tokio::test]
async fn cleanup_removes_old_acked_messages() -> anyhow::Result<()> {
    let store = make_test_store()?;
    let topic = "t".to_string();
    let group = "g".to_string();

    for i in 0..5 {
        store.append(&topic, 0, b"x").await?;
        store.mark_inflight(&topic, 0, &group, i, 0).await?;
        store.ack(&topic, 0, &group, i).await?;
    }

    store.cleanup_topic(&topic, 0).await?;

    let msgs = store.fetch_available(&topic, 0, &group, 0, 10).await?;
    assert!(msgs.is_empty());
    Ok(())
}

#[tokio::test]
async fn cleanup_blocked_by_other_group() -> anyhow::Result<()> {
    let store = make_test_store()?;
    let topic = "t".to_string();

    let g1 = "g1".to_string();
    let g2 = "g2".to_string();

    // messages 0..4
    for _i in 0..5 {
        store.append(&topic, 0, b"x").await?;
    }

    // Group1 acks all
    for i in 0..5 {
        store.mark_inflight(&topic, 0, &g1, i, 0).await?;
        store.ack(&topic, 0, &g1, i).await?;
    }

    // Group2 has not consumed anything
    // → cleanup should not delete any messages
    store.cleanup_topic(&topic, 0).await?;

    let msgs = store.fetch_available(&topic, 0, &g2, 0, 10).await?;
    assert_eq!(msgs.len(), 5);
    Ok(())
}

#[tokio::test]
async fn cleanup_preserves_remaining_offsets() -> anyhow::Result<()> {
    let store = make_test_store()?;
    let topic = "t".to_string();
    let g = "g".to_string();

    for _i in 0..10 {
        store.append(&topic, 0, b"x").await?;
    }

    // Ack first 7
    for i in 0..7 {
        store.mark_inflight(&topic, 0, &g, i, 0).await?;
        store.ack(&topic, 0, &g, i).await?;
    }

    store.cleanup_topic(&topic, 0).await?;

    let msgs = store.fetch_available(&topic, 0, &g, 0, 10).await?;
    let offsets: Vec<_> = msgs.iter().map(|m| m.message.offset).collect();

    assert_eq!(offsets, vec![7, 8, 9]);
    Ok(())
}

#[tokio::test]
async fn append_batch_basic() -> anyhow::Result<()> {
    let store = make_test_store()?;
    let topic = "t".to_string();

    let payloads = vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()];

    let offsets = store.append_batch(&topic, 0, &payloads).await?;
    assert_eq!(offsets, vec![0, 1, 2]);

    let msgs = store
        .fetch_available(&topic, 0, &"g".to_string(), 0, 10)
        .await?;

    assert_eq!(msgs.len(), 3);
    assert_eq!(msgs[0].message.payload, b"a");
    assert_eq!(msgs[1].message.payload, b"b");
    assert_eq!(msgs[2].message.payload, b"c");
    Ok(())
}

#[tokio::test]
async fn append_and_batch_interleave() -> anyhow::Result<()> {
    let store = make_test_store()?;
    let topic = "t".to_string();

    let off1 = store.append(&topic, 0, b"x").await?;
    assert_eq!(off1, 0);

    let offs = store
        .append_batch(&topic, 0, &[b"a".to_vec(), b"b".to_vec()])
        .await?;
    assert_eq!(offs, vec![1, 2]);

    let off2 = store.append(&topic, 0, b"z").await?;
    assert_eq!(off2, 3);

    let msgs = store
        .fetch_available(&topic, 0, &"g".to_string(), 0, 10)
        .await?;

    let payloads: Vec<_> = msgs.into_iter().map(|m| m.message.payload).collect();

    assert_eq!(
        payloads,
        vec![b"x".to_vec(), b"a".to_vec(), b"b".to_vec(), b"z".to_vec()]
    );
    Ok(())
}

#[tokio::test]
async fn append_batch_empty() -> anyhow::Result<()> {
    let store = make_test_store()?;
    let topic = "t".to_string();

    let offs = store.append_batch(&topic, 0, &[]).await?;
    assert!(offs.is_empty());
    Ok(())
}
