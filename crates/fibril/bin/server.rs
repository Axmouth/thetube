use std::{
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    sync::Arc,
};

use fibril_admin::{AdminConfig, AdminServer};
use fibril_broker::{Broker, BrokerConfig, coordination::NoopCoordination};
use fibril_metrics::{Metrics, MetricsConfig};
use fibril_protocol::v1::handler::run_server;
use fibril_storage::{observable_storage::ObservableStorage, rocksdb_store::RocksStorage};
use fibril_util::{StaticAuthHandler, init_tracing};

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

    let auth_handler = StaticAuthHandler::new("fibril".to_string(), "fibril".to_string());

    let broker_server_fut = run_server(
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::from([0, 0, 0, 0]), 9876)),
        broker,
        metrics.tcp(),
        metrics.connections(),
        Some(auth_handler.clone()),
    );

    let admin = AdminServer::new(
        metrics.clone(),
        AdminConfig {
            bind: "0.0.0.0:8080".into(),
            // auth: Some(auth_handler),
            auth: None,
        },
    );

    metrics.start(MetricsConfig {
        log_broker: true,
        log_storage: true,
        log_tcp: true,
    });

    let admin_server_dut = admin.run();

    let (broker_res, admin_res) = tokio::join!(broker_server_fut, admin_server_dut);
    broker_res?;
    admin_res?;

    Ok(())
}
