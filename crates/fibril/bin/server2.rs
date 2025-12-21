use fibril_broker::{BrokerConfig, ConsumerHandle, coordination::NoopCoordination};
use fibril_storage::rocksdb_store::RocksStorage;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    use fibril_broker::{Broker, ConsumerConfig};
    use std::{sync::Arc, time::Duration};
    let storage = RocksStorage::open("test_data/server2", true)?;
    let coord = NoopCoordination;
    let broker = Arc::new(Broker::try_new(storage, coord, BrokerConfig::default()).await?);

    let consumer = broker
        .subscribe(
            "topic",
            "group",
            ConsumerConfig { prefetch_count: 10 },
        )
        .await
        .unwrap();

    let ConsumerHandle {mut messages, ..} = consumer;

    tracing::info!("consumer started");
let handle = tokio::spawn(async move {
    while let Some(msg) = messages.recv().await {
                // intentionally do nothing
                let tag = msg.delivery_tag;
                tracing::info!("{}", tag);
            }
});

    let (publisher, _confstream) = broker.get_publisher("topic").await?;

    for i in 0..1000 {
        publisher.publish(vec![0; 1024 + i]).await?;
    }

    tracing::info!("sleeping");

    tokio::time::sleep(Duration::from_secs(30)).await;
handle.abort();

    // drop(messages);
    tracing::info!("consumer dropped, sleeping");

    Ok(())
}
