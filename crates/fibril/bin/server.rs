use std::sync::Arc;

use fibril_broker::{Broker, BrokerConfig, coordination::NoopCoordination};
use fibril_protocol::v1::{AuthHandler, handler::run_server};
use fibril_storage::rocksdb_store::RocksStorage;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let storage = RocksStorage::open("test_data/server", true)?;
    let coord = NoopCoordination;
    let broker = Arc::new(Broker::try_new(storage, coord, BrokerConfig::default()).await?);
    let auth_handler =  StaticAuthHandler {
        username: "fibril",
        password: "fibril",
    };
    run_server("0.0.0.0:9876", broker, Some(auth_handler)).await?;

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
