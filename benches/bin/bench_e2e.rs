use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Instant,
};

use fibril_broker::coordination::NoopCoordination;
use fibril_broker::{Broker, BrokerConfig, ConsumerHandle};
use fibril_broker::{AckRequest, ConsumerConfig};
use fibril_metrics::{Metrics, MetricsConfig};
use fibril_storage::{make_rocksdb_store, observable_storage::ObservableStorage};

use clap::{Parser, ValueEnum};
use fibril_util::init_tracing;

#[derive(Parser, Debug)]
#[command(name = "fibril")]
pub enum Command {
    Bench(BenchCmd),
}

#[derive(Parser, Debug)]
pub struct BenchCmd {
    #[command(subcommand)]
    pub mode: BenchMode,
}

#[derive(Parser, Debug)]
pub enum BenchMode {
    E2E(E2EBench),
}

#[derive(Parser, Debug)]
pub struct E2EBench {
    #[arg(long, default_value = "1000000")]
    pub messages: u64,

    #[arg(long, default_value = "4")]
    pub producers: usize,

    #[arg(long, default_value = "1")]
    pub consumers: usize,

    #[arg(long, default_value = "32")]
    pub payload_min: usize,

    #[arg(long, default_value = "256")]
    pub payload_max: usize,

    #[arg(long, default_value = "64")]
    pub batch_size: usize,

    #[arg(long, default_value = "1")]
    pub batch_timeout_ms: u64,

    /// Sync Writes for RocksDB backend
    #[arg(long, default_value = "false")]
    pub sync_write: bool,

    #[arg(long, default_value = "10000")]
    pub producer_inflight: usize,

    #[arg(long, default_value = "1")]
    pub report_interval_secs: u64,

    #[arg(long, value_enum, default_value = "async")]
    pub ack_mode: AckMode,
}

#[derive(ValueEnum, Clone, Debug, PartialEq, Eq)]
pub enum AckMode {
    Sync,
    Async,
    None,
}

#[repr(C)]
struct BenchPayload {
    msg_id: u64,
    producer_id: u32,
    payload: Vec<u8>,
}

fn make_payload(msg_id: u64, producer_id: u32, size: usize) -> Vec<u8> {
    let mut buf = Vec::with_capacity(12 + size);
    buf.extend_from_slice(&msg_id.to_be_bytes());
    buf.extend_from_slice(&producer_id.to_be_bytes());

    let mut body = vec![0u8; size];
    fastrand::fill(&mut body);
    buf.extend_from_slice(&body);

    buf
}

fn decode_payload(buf: &[u8]) -> (u64, u32) {
    let msg_id = u64::from_be_bytes(buf[0..8].try_into().unwrap());
    let producer = u32::from_be_bytes(buf[8..12].try_into().unwrap());
    (msg_id, producer)
}

struct BenchMetrics {
    published: AtomicU64,
    confirmed: AtomicU64,
    consumed: AtomicU64,
    acked: AtomicU64,
    inflight: AtomicUsize,
    duplicates: AtomicU64,

    start: Instant,

    publish_done_at: AtomicU64, // nanos since start
    consume_done_at: AtomicU64,
    ack_done_at: AtomicU64,
}

impl BenchMetrics {
    fn new() -> Self {
        Self {
            published: AtomicU64::new(0),
            confirmed: AtomicU64::new(0),
            consumed: AtomicU64::new(0),
            acked: AtomicU64::new(0),
            inflight: AtomicUsize::new(0),
            duplicates: AtomicU64::new(0),

            start: Instant::now(),
            publish_done_at: AtomicU64::new(0),
            consume_done_at: AtomicU64::new(0),
            ack_done_at: AtomicU64::new(0),
        }
    }
}

fn mark_done_once(slot: &AtomicU64, start: Instant) {
    let elapsed = start.elapsed().as_nanos() as u64;
    let _ = slot.compare_exchange(0, elapsed, Ordering::Relaxed, Ordering::Relaxed);
}

async fn producer_task(
    broker: Arc<Broker<NoopCoordination>>,
    topic: String,
    producer_id: u32,
    start_id: u64,
    count: u64,
    inflight_limit: usize,
    metrics: Arc<BenchMetrics>,
) {
    let (publisher, mut confirm_stream) = broker.get_publisher(&topic).await.unwrap();

    let metrics_clone = metrics.clone();

    tokio::spawn(async move {
        while let Some(res) = confirm_stream.recv_confirm().await {
            if res.is_ok() {
                metrics_clone.confirmed.fetch_add(1, Ordering::Relaxed);
                metrics_clone.inflight.fetch_sub(1, Ordering::Relaxed);
            }
        }
    });

    for i in 0..count {
        while metrics.inflight.load(Ordering::Relaxed) >= inflight_limit {
            tokio::task::yield_now().await;
        }

        let msg_id = start_id + i;
        let size = fastrand::usize(32..256);
        let payload = make_payload(msg_id, producer_id, size);

        metrics.inflight.fetch_add(1, Ordering::Relaxed);
        metrics.published.fetch_add(1, Ordering::Relaxed);

        publisher.publish(payload).await.unwrap();
    }

    if metrics.published.load(Ordering::Relaxed) >= start_id + count {
        mark_done_once(&metrics.publish_done_at, metrics.start);
    }
}

async fn consumer_task(
    mut consumer: ConsumerHandle,
    ack_mode: AckMode,
    metrics: Arc<BenchMetrics>,
    total: u64,
) {
    let (ackq_tx, mut ackq_rx) = tokio::sync::mpsc::channel::<u64>(10_000);

    // One task that actually sends acks
    let acker = consumer.acker.clone();
    let metrics2 = metrics.clone();
    tokio::spawn(async move {
        while let Some(off) = ackq_rx.recv().await {
            let req = AckRequest { delivery_tag: off };
            if acker.send(req).await.is_ok() {
                metrics2.acked.fetch_add(1, Ordering::Relaxed);
            }
        }
    });

    while let Some(msg) = consumer.messages.recv().await {
        let c = metrics.consumed.fetch_add(1, Ordering::Relaxed) + 1;
        if c == total {
            mark_done_once(&metrics.consume_done_at, metrics.start);
        }

        match ack_mode {
            AckMode::Sync => {
                let req = AckRequest {
                    delivery_tag: msg.delivery_tag,
                };
                let _ = consumer.acker.try_send(req);
                let a = metrics.acked.fetch_add(1, Ordering::Relaxed);
                if a == total {
                    mark_done_once(&metrics.ack_done_at, metrics.start);
                }
            }
            AckMode::Async => {
                // queue ack; backpressure here is fine
                ackq_tx.send(msg.delivery_tag).await.unwrap();
            }
            AckMode::None => {}
        }
    }
}

async fn reporter(
    metrics: Arc<BenchMetrics>,
    broker: Arc<Broker<NoopCoordination>>,
    topic: String,
    interval: u64,
    total: u64,
) {
    let mut ticker = tokio::time::interval(std::time::Duration::from_secs(interval));
    let upper = broker.debug_upper(&topic, 0).await;

    loop {
        ticker.tick().await;
        let acked = metrics.acked.load(Ordering::Relaxed);
        tracing::info!(
            "pub={} conf={} inflight={} cons={} ack={} dup={} upper={}",
            metrics.published.load(Ordering::Relaxed),
            metrics.confirmed.load(Ordering::Relaxed),
            metrics.inflight.load(Ordering::Relaxed),
            metrics.consumed.load(Ordering::Relaxed),
            acked,
            metrics.duplicates.load(Ordering::Relaxed),
            upper
        );

        if acked >= total {
            break;
        }
    }
}

async fn make_broker_with_cfg(cmd: &E2EBench) -> Broker<NoopCoordination> {
    let mut cfg = BrokerConfig {
        publish_batch_size: cmd.batch_size,
        publish_batch_timeout_ms: cmd.batch_timeout_ms,
        ack_batch_size: 512,
        ack_batch_timeout_ms: 20,
        inflight_batch_size: 512,
        inflight_batch_timeout_ms: 20,
        inflight_ttl_secs: 60,
        cleanup_interval_secs: 5,
        ..Default::default()
    };

    // IMPORTANT: inflight TTL must exceed bench duration
    cfg.inflight_ttl_secs = 60;

    let db_path = format!("bench_data/{}", fastrand::u64(..)); // stable path for E2E
    let _ = std::fs::remove_dir_all(&db_path);
    std::fs::create_dir_all(&db_path).unwrap();

    let store = make_rocksdb_store(&db_path, cmd.sync_write).unwrap();
    let metrics = Metrics::new(60 * 60);
    let store = ObservableStorage::new(store, metrics.storage());
    let coord = NoopCoordination {};

    let broker = Broker::try_new(store, coord, metrics.broker(), cfg).await.unwrap();

    metrics.start(MetricsConfig {
        log_broker: true,
        log_storage: true,
        log_tcp: false,
    });

    broker
}

async fn run_e2e_bench(cmd: E2EBench) {
    let broker = make_broker_with_cfg(&cmd).await;
    let broker = Arc::new(broker);
    let broker_clone = broker.clone();

    let metrics = Arc::new(BenchMetrics::new());

    let topic = format!("bench_topic_{}", fastrand::u64(..));

    // Consumers
    for _ in 0..cmd.consumers {
        let consumer = broker
            .subscribe(
                &topic,
                "bench_group",
                ConsumerConfig::default().with_prefetch_count(8192),
            )
            .await
            .unwrap();
        tokio::spawn(consumer_task(
            consumer,
            cmd.ack_mode.clone(),
            metrics.clone(),
            cmd.messages,
        ));
    }

    // Producers
    let per_producer = cmd.messages / cmd.producers as u64;

    for p in 0..cmd.producers {
        tokio::spawn(producer_task(
            broker.clone(),
            topic.clone(),
            p as u32,
            p as u64 * per_producer,
            per_producer,
            cmd.producer_inflight,
            metrics.clone(),
        ));
    }

    // Reporter
    let handle = tokio::spawn(reporter(
        metrics.clone(),
        broker.clone(),
        topic.clone(),
        cmd.report_interval_secs,
        cmd.messages
    ));

    // Wait until done
    while metrics.acked.load(Ordering::Relaxed) < cmd.messages {
        tokio::time::sleep(std::time::Duration::from_millis(1001)).await;
    }

    handle.await.unwrap();

    let total = cmd.messages as f64;

    let pub_ns = metrics.publish_done_at.load(Ordering::Relaxed);
    let con_ns = metrics.consume_done_at.load(Ordering::Relaxed);
    let ack_ns = metrics.ack_done_at.load(Ordering::Relaxed);

    tracing::info!("\n");
    tracing::info!("=== FINAL THROUGHPUT ===");

    if pub_ns > 0 {
        let secs = pub_ns as f64 / 1e9;
        tracing::info!("Published: {:.0} msg/s", total / secs);
    }

    if con_ns > 0 {
        let secs = con_ns as f64 / 1e9;
        tracing::info!("Consumed:  {:.0} msg/s", total / secs);
    }

    if ack_ns > 0 {
        let secs = ack_ns as f64 / 1e9;
        tracing::info!("Acked:     {:.0} msg/s", total / secs);
    }

    broker.flush_storage().await.unwrap();
    let partition = 0;
    broker.forced_cleanup(&topic, partition).await.unwrap();

    broker_clone.dump_meta_keys().await;

    tracing::info!("Bench complete.");
}

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() {
    init_tracing();

    let cmd = Command::parse();

    match cmd {
        Command::Bench(b) => match b.mode {
            BenchMode::E2E(e2e) => {
                tracing::info!("Starting E2E bench: {:#?}", e2e);
                run_e2e_bench(e2e).await;
            }
        },
    }
}
