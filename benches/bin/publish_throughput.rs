use std::sync::Arc;

use fibril_broker::{Broker, BrokerConfig, coordination::NoopCoordination};
use fibril_metrics::Metrics;
use fibril_storage::make_rocksdb_store;

use clap::Parser;
use fibril_util::init_tracing;
use tokio::time::Instant;

/// Benchmark publisher load.
#[derive(Parser, Debug)]
struct Args {
    /// Number of messages to publish
    #[arg(long, default_value = "1000000")]
    messages: usize,

    /// Parallel publisher tasks
    #[arg(long, default_value = "50")]
    parallelism: usize,

    /// Maximum payload size (randomized 32..max)
    #[arg(long, default_value = "256")]
    max_payload: usize,

    /// Batch size for broker
    #[arg(long, default_value = "64")]
    batch_size: usize,

    /// Batch timeout in ms
    #[arg(long, default_value = "1")]
    batch_timeout_ms: u64,

    /// Path Writes for RocksDB backend
    #[arg(long, default_value = "test_data/bench_data")]
    db_path: String,

    /// Sync Writes for RocksDB backend
    #[arg(long, default_value = "false")]
    pub sync_write: bool,
}

#[tokio::main]
async fn main() {
    init_tracing();

    let args = Args::parse();

    // Set up config
    let cfg = BrokerConfig {
        publish_batch_size: args.batch_size,
        publish_batch_timeout_ms: args.batch_timeout_ms,
        ..Default::default()
    };

    // RocksDB setup
    std::fs::create_dir_all(&args.db_path).unwrap();
    let storage = make_rocksdb_store(&args.db_path, args.sync_write).unwrap();
    let metrics = Metrics::new(60 * 60);

    let broker = Arc::new(
        Broker::try_new(storage, NoopCoordination {}, metrics.broker(), cfg)
            .await
            .unwrap(),
    );

    tracing::info!("Benchmark: Publishing {} messages...", args.messages);
    tracing::info!(
        "parallel={}, batch_size={}, timeout={}ms, max_payload={}",
        args.parallelism, args.batch_size, args.batch_timeout_ms, args.max_payload
    );

    let mut tasks = Vec::new();
    let msgs_per_worker = args.messages / args.parallelism;

    let start = Instant::now();

    for _ in 0..args.parallelism {
        let max_payload = args.max_payload;
        let (publisher, mut confirm_stream) = broker.get_publisher("bench").await.unwrap();
        tasks.push(tokio::spawn(async move {
            for _ in 0..msgs_per_worker {
                let size = fastrand::usize(32..max_payload);
                let mut buf = vec![0u8; size];
                fastrand::fill(&mut buf);
                publisher.publish(buf).await.unwrap();
            }
        }));

        tokio::spawn(async move {
            let mut confirmed = 0;
            while let Some(res) = confirm_stream.recv_confirm().await {
                if res.is_ok() {
                    confirmed += 1;
                }
                if confirmed >= msgs_per_worker {
                    break;
                }
            }

            let finish_time = Instant::now();
            tracing::info!(
                "Publisher confirmed {} messages in {:.2} seconds",
                confirmed,
                (finish_time - start).as_secs_f64()
            );
        });
    }

    for t in tasks {
        t.await.unwrap();
    }

    let elapsed = start.elapsed().as_secs_f64();
    let rate = (args.messages as f64) / elapsed;

    tracing::info!("Total time: {:.2} s", elapsed);
    tracing::info!("Throughput: {:.2} messages/sec", rate);
}
