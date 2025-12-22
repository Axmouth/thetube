use std::{
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    sync::Arc,
};

use fibril_broker::{Broker, BrokerConfig, coordination::NoopCoordination};
use fibril_metrics::{Metrics, MetricsConfig};
use fibril_protocol::v1::{AuthHandler, handler::run_server};
use fibril_storage::{observable_storage::ObservableStorage, rocksdb_store::RocksStorage};
use fibril_util::init_tracing;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    // TODO configurable stuff
    let storage = RocksStorage::open("test_data/server", true)?;
    let metrics = Metrics::new(3 * 60 * 60); // 3 hours
    let observable_storage = ObservableStorage::new(storage, metrics.storage());
    let coord = NoopCoordination;
    let broker = Arc::new(
        Broker::try_new(
            observable_storage,
            coord,
            metrics.broker(),
            BrokerConfig {
                ack_batch_timeout_ms: 2,
                ack_batch_size: 128,
                ..BrokerConfig::default()
            },
        )
        .await?,
    );
    let auth_handler = StaticAuthHandler {
        username: "fibril",
        password: "fibril",
    };

    let server_fut = run_server(
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::from([0, 0, 0, 0]), 9876)),
        broker,
        metrics.tcp(),
        Some(auth_handler),
    );

    metrics.start(MetricsConfig {
        log_broker: true,
        log_storage: true,
        log_tcp: true,
    });

    server_fut.await?;

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
