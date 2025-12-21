use std::{
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    sync::{Arc, atomic::Ordering},
};

use fibril_broker::{Broker, BrokerConfig, coordination::NoopCoordination};
use fibril_protocol::v1::{AuthHandler, handler::run_server};
use fibril_storage::{
    observable_storage::{ObservableStorage, StorageStats},
    rocksdb_store::RocksStorage,
};
use fibril_util::init_tracing;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    // TODO configurable stuff
    let storage = RocksStorage::open("test_data/server", true)?;
    let stats = StorageStats::new();
    let stats_clone = Arc::clone(&stats);
    let observable_storage = ObservableStorage::new(storage, stats);
    let coord = NoopCoordination;
    let broker =
        Arc::new(Broker::try_new(observable_storage, coord, BrokerConfig::default()).await?);
    let auth_handler = StaticAuthHandler {
        username: "fibril",
        password: "fibril",
    };

    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;

            let writes_1m = stats_clone.writes.ops.sum_last(60);
            let reads_1m = stats_clone.reads.ops.sum_last(60);

            let total_writes = stats_clone.writes.total.load(Ordering::Relaxed);
            let total_reads = stats_clone.reads.total.load(Ordering::Relaxed);

            tracing::info!(
                "[storage] writes/s(1m): {:.1}, reads/s(1m): {:.1}, total_writes: {}",
                writes_1m as f64 / 60.0,
                reads_1m as f64 / 60.0,
                total_writes,
            );
        }
    });

    run_server(
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::from([0, 0, 0, 0]), 9876)),
        broker,
        Some(auth_handler),
    )
    .await?;

    Ok(())
}

#[derive(Debug, Clone)]
pub struct StaticAuthHandler {
    username: &'static str,
    password: &'static str,
}

impl StaticAuthHandler {
    pub fn new(username: &'static str, password: &'static str) -> Self {
        Self { username, password }
    }
}

#[async_trait::async_trait]
impl AuthHandler for StaticAuthHandler {
    async fn verify(&self, username: &str, password: &str) -> bool {
        username.to_lowercase() == self.username && password == self.password
    }
}
