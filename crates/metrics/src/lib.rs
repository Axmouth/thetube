use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use serde::Serialize;
use serde_json::json;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use uuid::Uuid;

// TODO: Save ip/host per connection (to display connections list)
// TODO: Save above connection info per sub too(topic, group too), plus ways to calculate uptime for each
// TODO: also similar data per publisher
// TODO: Messages per unit of time for each subs and pubs too
// TODO: Optionally persist stats too, clean up what is old, user can have a minimal idea what led uo to crashes without complex external systems

#[derive(Debug)]
pub struct RollingCounter {
    buckets: Vec<AtomicU64>,
    last_tick: AtomicU64,
    resolution_secs: u64,
}

impl RollingCounter {
    pub fn new(resolution_secs: u64, bucket_count: usize) -> Self {
        Self {
            buckets: (0..bucket_count).map(|_| AtomicU64::new(0)).collect(),
            last_tick: AtomicU64::new(0),
            resolution_secs,
        }
    }

    #[inline]
    pub fn incr(&self) {
        let now = current_epoch_secs() / self.resolution_secs;
        let idx = (now as usize) % self.buckets.len();

        let last = self.last_tick.swap(now, Ordering::Relaxed);
        if last != now {
            self.buckets[idx].store(0, Ordering::Relaxed);
        }

        self.buckets[idx].fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn incr_many(&self, many: u64) {
        let now = current_epoch_secs() / self.resolution_secs;
        let idx = (now as usize) % self.buckets.len();

        let last = self.last_tick.swap(now, Ordering::Relaxed);
        if last != now {
            self.buckets[idx].store(0, Ordering::Relaxed);
        }

        self.buckets[idx].fetch_add(many, Ordering::Relaxed);
    }

    pub fn sum_last(&self, seconds: usize) -> u64 {
        let now = current_epoch_secs() / self.resolution_secs;
        let mut sum = 0;

        for i in 0..seconds.min(self.buckets.len()) {
            let idx =
                ((now as isize - i as isize).rem_euclid(self.buckets.len() as isize)) as usize;
            sum += self.buckets[idx].load(Ordering::Relaxed);
        }

        sum
    }

    pub fn rate_per_sec(&self, window_secs: usize) -> f64 {
        self.sum_last(window_secs) as f64 / window_secs.min(self.buckets.len().max(1)) as f64
    }
}

#[inline]
fn current_epoch_secs() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

#[derive(Debug)]
pub struct LatencyStats {
    pub count: AtomicU64,
    pub total_micros: AtomicU64,
}

impl Default for LatencyStats {
    fn default() -> Self {
        Self {
            count: AtomicU64::new(0),
            total_micros: AtomicU64::new(0),
        }
    }
}

impl LatencyStats {
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    pub fn observe(&self, d: Duration) {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.total_micros
            .fetch_add(d.as_micros() as u64, Ordering::Relaxed);
    }

    pub fn avg_micros(&self) -> Option<f64> {
        let count = self.count.load(Ordering::Relaxed);
        if count == 0 {
            None
        } else {
            Some(self.total_micros.load(Ordering::Relaxed) as f64 / count as f64)
        }
    }
}

pub struct Timer<'a> {
    start: Instant,
    stats: &'a LatencyStats,
}

impl<'a> Timer<'a> {
    #[inline]
    pub fn new(stats: &'a LatencyStats) -> Self {
        Self {
            start: Instant::now(),
            stats,
        }
    }
}

impl Drop for Timer<'_> {
    #[inline]
    fn drop(&mut self) {
        self.stats.observe(self.start.elapsed());
    }
}

#[derive(Debug)]
pub struct OpStats {
    pub ops: RollingCounter,
    pub latency: LatencyStats,
    pub total: AtomicU64,
    pub errors: AtomicU64,
}

impl OpStats {
    pub fn new(bucket_count: usize) -> Self {
        Self {
            ops: RollingCounter::new(1, bucket_count),
            latency: LatencyStats::new(),
            total: AtomicU64::new(0),
            errors: AtomicU64::new(0),
        }
    }

    #[inline]
    pub fn incr(&self) {
        self.ops.incr();
        self.total.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn incr_many(&self, many: u64) {
        self.ops.incr_many(many);
        self.total.fetch_add(many, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_result<T, E>(&self, res: &Result<T, E>) {
        self.incr();
        if res.is_err() {
            self.errors.fetch_add(1, Ordering::Relaxed);
        }
    }
}

#[derive(Debug)]
pub struct BatchStats {
    pub batches: OpStats,
    pub items_total: AtomicU64,
    pub bytes_total: AtomicU64,
}

impl BatchStats {
    pub fn new(bucket_count: usize) -> Self {
        Self {
            batches: OpStats::new(bucket_count),
            items_total: AtomicU64::new(0),
            bytes_total: AtomicU64::new(0),
        }
    }

    #[inline]
    pub fn observe(&self, items: usize, bytes: usize) {
        self.batches.incr();
        self.items_total.fetch_add(items as u64, Ordering::Relaxed);
        self.bytes_total.fetch_add(bytes as u64, Ordering::Relaxed);
    }
}

#[derive(Debug)]
pub struct StorageStats {
    pub writes: OpStats,
    pub reads: OpStats,
    pub acks: OpStats,
    pub inflight: OpStats,
    pub maintenance: OpStats,
    pub control_plane: OpStats,
    pub redelivery: OpStats,

    pub append_batches: BatchStats,
    pub ack_batches: BatchStats,
}

impl StorageStats {
    pub fn new(buckets: usize) -> Arc<Self> {
        Arc::new(Self {
            writes: OpStats::new(buckets),
            reads: OpStats::new(buckets),
            acks: OpStats::new(buckets),
            inflight: OpStats::new(buckets),
            maintenance: OpStats::new(buckets),
            control_plane: OpStats::new(buckets),
            redelivery: OpStats::new(buckets),

            append_batches: BatchStats::new(buckets),
            ack_batches: BatchStats::new(buckets),
        })
    }

    #[inline]
    pub fn record_ack(&self) {
        self.acks.incr();
    }

    #[inline]
    pub fn record_acks(&self, acks: u64) {
        self.acks.incr_many(acks);
    }

    #[inline]
    pub fn record_inflight(&self) {
        self.inflight.incr();
    }

    #[inline]
    pub fn record_inflights(&self, inflight: u64) {
        self.inflight.incr_many(inflight);
    }

    #[inline]
    pub fn record_write(&self) {
        self.writes.incr();
    }

    #[inline]
    pub fn record_writes(&self, writes: u64) {
        self.writes.incr_many(writes);
    }

    #[inline]
    pub fn record_read(&self) {
        self.reads.incr();
    }

    #[inline]
    pub fn record_reads(&self, reads: u64) {
        self.reads.incr_many(reads);
    }

    #[inline]
    pub fn record_append_batch(&self, items: usize, bytes: usize) {
        self.append_batches.observe(items, bytes);
    }

    pub fn snapshot(&self) -> StorageStatsSnapshot {
        let reads_1m = self.reads.ops.rate_per_sec(60);
        let writes_1m = self.writes.ops.rate_per_sec(60);

        StorageStatsSnapshot {
            reads_per_sec_1m: reads_1m,
            writes_per_sec_1m: writes_1m,
            total_reads: self.reads.total.load(Ordering::Relaxed),
            total_writes: self.writes.total.load(Ordering::Relaxed),

            avg_read_latency_ms: self.reads.latency.avg_micros().map(|v| v / 1000.0),

            avg_write_latency_ms: self.writes.latency.avg_micros().map(|v| v / 1000.0),

            avg_ack_latency_ms: self.acks.latency.avg_micros().map(|v| v / 1000.0),

            avg_append_batch_latency_ms: self
                .append_batches
                .batches
                .latency
                .avg_micros()
                .map(|v| v / 1000.0),

            avg_ack_batch_latency_ms: self
                .ack_batches
                .batches
                .latency
                .avg_micros()
                .map(|v| v / 1000.0),

            avg_flush_latency_ms: self.maintenance.latency.avg_micros().map(|v| v / 1000.0),
        }
    }
}

#[derive(Serialize)]
pub struct StorageStatsSnapshot {
    pub reads_per_sec_1m: f64,
    pub writes_per_sec_1m: f64,
    pub total_reads: u64,
    pub total_writes: u64,

    pub avg_read_latency_ms: Option<f64>,
    pub avg_write_latency_ms: Option<f64>,
    pub avg_ack_latency_ms: Option<f64>,
    pub avg_append_batch_latency_ms: Option<f64>,

    pub avg_ack_batch_latency_ms: Option<f64>,
    pub avg_flush_latency_ms: Option<f64>,
}

#[derive(Debug)]
pub struct BrokerStats {
    pub delivered: OpStats,
    pub published: OpStats,
    pub acked: OpStats,
    pub redelivered: OpStats,
    pub expired: OpStats,

    pub publish_batches: BatchStats,
    pub ack_batches: BatchStats,
}

impl BrokerStats {
    pub fn new(buckets: usize) -> Arc<Self> {
        Arc::new(Self {
            delivered: OpStats::new(buckets),
            published: OpStats::new(buckets),
            acked: OpStats::new(buckets),
            redelivered: OpStats::new(buckets),
            expired: OpStats::new(buckets),
            publish_batches: BatchStats::new(buckets),
            ack_batches: BatchStats::new(buckets),
        })
    }

    #[inline]
    pub fn delivered(&self) {
        self.delivered.incr();
    }

    #[inline]
    pub fn acked(&self) {
        self.acked.incr();
    }

    #[inline]
    pub fn acked_many(&self, acked: u64) {
        self.acked.incr_many(acked);
    }

    #[inline]
    pub fn published(&self) {
        self.published.incr();
    }

    #[inline]
    pub fn published_many(&self, published: u64) {
        self.published.incr_many(published);
    }

    #[inline]
    pub fn redelivered(&self) {
        self.redelivered.incr();
    }

    #[inline]
    pub fn expired(&self) {
        self.expired.incr();
    }

    #[inline]
    pub fn ack_batch(&self, batch_size: usize, bytes: usize) {
        self.ack_batches.observe(batch_size, bytes);
    }

    #[inline]
    pub fn publish_batch(&self, batch_size: usize, bytes: usize) {
        self.publish_batches.observe(batch_size, bytes);
    }

    pub fn snapshot(&self) -> BrokerStatsSnapshot {
        let window = 60;

        let delivered = self.delivered.ops.sum_last(window);
        let published = self.published.ops.sum_last(window);
        let acked = self.acked.ops.sum_last(window);
        let redelivered = self.redelivered.ops.sum_last(window);
        let expired = self.expired.ops.sum_last(window);

        BrokerStatsSnapshot {
            delivered_per_sec_1m: delivered as f64 / window as f64,
            published_per_sec_1m: published as f64 / window as f64,
            acked_per_sec_1m: acked as f64 / window as f64,
            redelivered_per_sec_1m: redelivered as f64 / window as f64,
            expired_per_sec_1m: expired as f64 / window as f64,

            avg_publish_batch_size: {
                let total = self.publish_batches.items_total.load(Ordering::Relaxed);
                let batches = self.publish_batches.batches.total.load(Ordering::Relaxed);
                if batches == 0 {
                    None
                } else {
                    Some(total as f64 / batches as f64)
                }
            },

            avg_ack_batch_size: {
                let total = self.ack_batches.items_total.load(Ordering::Relaxed);
                let batches = self.ack_batches.batches.total.load(Ordering::Relaxed);
                if batches == 0 {
                    None
                } else {
                    Some(total as f64 / batches as f64)
                }
            },

            total_delivered: self.delivered.total.load(Ordering::Relaxed),
            total_acked: self.acked.total.load(Ordering::Relaxed),
            total_redelivered: self.redelivered.total.load(Ordering::Relaxed),
            total_expired: self.expired.total.load(Ordering::Relaxed),
        }
    }
}

#[derive(Serialize)]
pub struct BrokerStatsSnapshot {
    pub delivered_per_sec_1m: f64,
    pub published_per_sec_1m: f64,
    pub acked_per_sec_1m: f64,
    pub redelivered_per_sec_1m: f64,
    pub expired_per_sec_1m: f64,

    pub avg_publish_batch_size: Option<f64>,
    pub avg_ack_batch_size: Option<f64>,

    pub total_delivered: u64,
    pub total_acked: u64,
    pub total_redelivered: u64,
    pub total_expired: u64,
}

pub struct TcpStats {
    pub connections: OpStats,
    pub disconnections: OpStats,
    pub bytes_in: AtomicU64,
    pub bytes_out: AtomicU64,
    pub errors: AtomicU64,
}

impl TcpStats {
    pub fn new(buckets: usize) -> Arc<Self> {
        Arc::new(Self {
            connections: OpStats::new(buckets),
            disconnections: OpStats::new(buckets),
            bytes_in: AtomicU64::new(0),
            bytes_out: AtomicU64::new(0),
            errors: AtomicU64::new(0),
        })
    }

    #[inline]
    pub fn connection_opened(&self) {
        self.connections.incr();
    }

    #[inline]
    pub fn connection_closed(&self) {
        self.disconnections.incr();
    }

    #[inline]
    pub fn bytes_in(&self, n: u64) {
        self.bytes_in.fetch_add(n, Ordering::Relaxed);
    }

    #[inline]
    pub fn bytes_out(&self, n: u64) {
        self.bytes_out.fetch_add(n, Ordering::Relaxed);
    }

    #[inline]
    pub fn error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> TcpStatsSnapshot {
        let window = 60;

        let connections = self.connections.ops.sum_last(window);
        let disconnections = self.disconnections.ops.sum_last(window);

        let bytes_in = self.bytes_in.load(Ordering::Relaxed);
        let bytes_out = self.bytes_out.load(Ordering::Relaxed);

        // crude but honest: derive rates from totals
        // (can switch to rolling counters later if needed)
        TcpStatsSnapshot {
            connections_per_sec_1m: connections as f64 / window as f64,
            disconnections_per_sec_1m: disconnections as f64 / window as f64,
            total_connections: self.connections.total.load(Ordering::Relaxed),

            bytes_in_per_sec_1m: bytes_in as f64 / window as f64,
            bytes_out_per_sec_1m: bytes_out as f64 / window as f64,

            total_bytes_in: bytes_in,
            total_bytes_out: bytes_out,

            errors_total: self.errors.load(Ordering::Relaxed),
        }
    }
}

#[derive(Serialize)]
pub struct TcpStatsSnapshot {
    pub connections_per_sec_1m: f64,
    pub disconnections_per_sec_1m: f64,
    pub total_connections: u64,

    pub bytes_in_per_sec_1m: f64,
    pub bytes_out_per_sec_1m: f64,

    pub total_bytes_in: u64,
    pub total_bytes_out: u64,

    pub errors_total: u64,
}

#[derive(Clone)]
pub struct Metrics {
    inner: Arc<MetricsInner>,
}

pub struct MetricsHandle {
    metrics: Metrics,
    runtime: MetricsRuntime,
}

impl MetricsHandle {
    pub fn metrics(&self) -> &Metrics {
        &self.metrics
    }

    pub async fn shutdown(self) {
        self.runtime.shutdown().await;
    }
}

struct MetricsInner {
    pub storage: Arc<StorageStats>,
    pub broker: Arc<BrokerStats>,
    pub tcp: Arc<TcpStats>,
    pub conn: Arc<ConnectionStats>,
    // later: protocol, client, etc
}

impl Metrics {
    pub fn new(buckets: usize) -> Self {
        Self {
            inner: Arc::new(MetricsInner {
                storage: StorageStats::new(buckets),
                broker: BrokerStats::new(buckets),
                tcp: TcpStats::new(buckets),
                conn: ConnectionStats::new(),
            }),
        }
    }

    pub fn start(self, config: MetricsConfig) -> MetricsHandle {
        let runtime = MetricsRuntime::start(self.clone(), config);
        MetricsHandle {
            metrics: self,
            runtime,
        }
    }

    pub fn storage(&self) -> Arc<StorageStats> {
        self.inner.storage.clone()
    }

    pub fn broker(&self) -> Arc<BrokerStats> {
        self.inner.broker.clone()
    }

    pub fn tcp(&self) -> Arc<TcpStats> {
        self.inner.tcp.clone()
    }

    pub fn connections(&self) -> Arc<ConnectionStats> {
        self.inner.conn.clone()
    }
}

pub struct MetricsRuntime {
    shutdown: ShutdownSignal,
    handles: Vec<JoinHandle<()>>,
}

impl MetricsRuntime {
    pub fn start(metrics: Metrics, config: MetricsConfig) -> Self {
        let shutdown = ShutdownSignal::new();
        let mut handles = Vec::new();
        let dur = Duration::from_secs(10);

        if config.log_storage {
            handles.push(tokio::spawn(run_storage_logger(
                metrics.storage(),
                dur,
                shutdown.subscribe(),
            )));
        }

        if config.log_broker {
            handles.push(tokio::spawn(run_broker_logger(
                metrics.broker(),
                dur,
                shutdown.subscribe(),
            )));
        }

        if config.log_tcp {
            handles.push(tokio::spawn(run_tcp_logger(
                metrics.tcp(),
                dur,
                shutdown.subscribe(),
            )));
        }

        Self { shutdown, handles }
    }

    pub async fn shutdown(self) {
        self.shutdown.signal();
        for h in self.handles {
            let _ = h.await;
        }
    }
}

fn effective_window(max: usize) -> usize {
    let now = current_epoch_secs() as usize;
    now.min(max).max(1)
}

pub struct MetricsConfig {
    pub log_storage: bool,
    pub log_broker: bool,
    pub log_tcp: bool,
}

#[inline]
fn round1(v: f64) -> f64 {
    (v * 10.0).round() / 10.0
}

#[inline]
fn fmt2(v: f64) -> f64 {
    (v * 100.0).round() / 100.0
}

#[inline]
fn round0(v: f64) -> u64 {
    v.round() as u64
}

pub async fn run_storage_logger(
    stats: Arc<StorageStats>,
    interval: Duration,
    mut shutdown: ShutdownSignal,
) {
    loop {
        tokio::select! {
            _ = tokio::time::sleep(interval) => {
                let snap = stats.snapshot();
                tracing::info!(
                    reads_s = round1(snap.reads_per_sec_1m),
                    writes_s = round1(snap.writes_per_sec_1m),

                    read_ms = snap.avg_read_latency_ms.map(fmt2),
                    write_ms = snap.avg_write_latency_ms.map(fmt2),
                    ack_ms = snap.avg_ack_latency_ms.map(fmt2),
                    append_batch_ms = snap.avg_append_batch_latency_ms.map(fmt2),
                    ack_batch_ms = snap.avg_ack_batch_latency_ms.map(fmt2),

                    total_reads = snap.total_reads,
                    total_writes = snap.total_writes,
                    "[storage]"
                );
            }
            _ = shutdown.recv() => {
                break;
            }
        }
    }
}

pub async fn run_broker_logger(
    stats: Arc<BrokerStats>,
    interval: Duration,
    mut shutdown: ShutdownSignal,
) {
    // TODO: add mark inflight batch?
    loop {
        tokio::select! {
            _ = tokio::time::sleep(interval) => {
                let snap = stats.snapshot();
                tracing::info!(
                    delivered_s = round1(snap.delivered_per_sec_1m),
                    published_s = round1(snap.published_per_sec_1m),
                    acked_s = round1(snap.acked_per_sec_1m),

                    pub_batch_size = snap.avg_publish_batch_size.map(fmt2),
                    ack_batch_size = snap.avg_ack_batch_size.map(fmt2),

                    redelivered_s = round1(snap.redelivered_per_sec_1m),
                    expired_s = round1(snap.expired_per_sec_1m),
                    total_delivered = snap.total_delivered,
                    total_acked = snap.total_acked,
                    "[broker]"
                );
            }
            _ = shutdown.recv() => {
                break;
            }
        }
    }
}

pub async fn run_tcp_logger(
    stats: Arc<TcpStats>,
    interval: Duration,
    mut shutdown: ShutdownSignal,
) {
    loop {
        tokio::select! {
            _ = tokio::time::sleep(interval) => {
                let snap = stats.snapshot();
                tracing::info!(
                    connections_s = round1(snap.connections_per_sec_1m),
                    disconnections_s = round1(snap.disconnections_per_sec_1m),
                    bytes_in_s = round1(snap.bytes_in_per_sec_1m),
                    bytes_out_s = round1(snap.bytes_out_per_sec_1m),
                    total_connections = snap.total_connections,
                    total_bytes_in = snap.total_bytes_in,
                    total_bytes_out = snap.total_bytes_out,
                    errors = snap.errors_total,
                    "[tcp]"
                );
            }
            _ = shutdown.recv() => {
                break;
            }
        }
    }
}

pub struct ShutdownSignal {
    tx: watch::Sender<bool>,
    rx: watch::Receiver<bool>,
}

impl ShutdownSignal {
    pub fn new() -> Self {
        let (tx, rx) = watch::channel(false);
        Self { tx, rx }
    }

    pub fn subscribe(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            rx: self.rx.clone(),
        }
    }

    pub async fn recv(&mut self) {
        while !*self.rx.borrow() {
            if self.rx.changed().await.is_err() {
                break;
            }
        }
    }

    pub fn signal(&self) {
        let _ = self.tx.send(true);
    }
}

type ConnId = Uuid;
type SubId = Uuid;
type Topic = String;
type Group = String;

struct PublisherInfo {
    peer_addr: SocketAddr,
    connected_at: Instant,
    last_publish_at: AtomicU64,
}

struct SubInfo {
    sub_id: Uuid,
    topic: Topic,
    group: Group,
    connected_at: Instant,
    auto_ack: bool,
}

impl SubInfo {
    pub fn new(
        sub_id: SubId,
        topic: Topic,
        group: Group,
        connected_at: Instant,
        auto_ack: bool,
    ) -> Self {
        Self {
            sub_id,
            topic,
            group,
            connected_at,
            auto_ack,
        }
    }
}

struct ConnectionState {
    conn_id: ConnId,
    peer: SocketAddr,
    connected_at: Instant,
    authenticated: bool,
    subs: DashMap<SubId, SubInfo>,
}

impl ConnectionState {
    pub fn new(
        conn_id: ConnId,
        peer: SocketAddr,
        connected_at: Instant,
        authenticated: bool,
    ) -> Self {
        Self {
            conn_id,
            peer,
            connected_at,
            authenticated,
            subs: DashMap::new(),
        }
    }

    pub fn add_sub(
        &self,
        topic: Topic,
        group: Group,
        connected_at: Instant,
        auto_ack: bool,
    ) -> SubId {
        let sub_id = Uuid::now_v7();
        self.subs.insert(
            sub_id,
            SubInfo {
                sub_id,
                topic,
                group,
                connected_at,
                auto_ack,
            },
        );

        sub_id
    }

    pub fn remove_sub(&self, key: &SubId) -> bool {
        self.subs.remove(key).is_some()
    }
}

pub struct ConnectionStats {
    connections: DashMap<ConnId, ConnectionState>,
}

impl ConnectionStats {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            connections: DashMap::new(),
        })
    }

    pub fn add_connection(
        &self,
        peer: SocketAddr,
        connected_at: Instant,
        authenticated: bool,
    ) -> ConnId {
        let conn_id = Uuid::now_v7();
        self.connections.insert(
            conn_id,
            ConnectionState::new(conn_id, peer, connected_at, authenticated),
        );

        conn_id
    }

    pub fn set_connection_auth(&self, key: &Uuid, auth: bool) -> bool {
        if let Some(mut conn) = self.connections.get_mut(key) {
            conn.authenticated = auth;
            true
        } else {
            false
        }
    }

    pub fn add_sub(
        &self,
        conn_id: &ConnId,
        topic: Topic,
        group: Group,
        connected_at: Instant,
        auto_ack: bool,
    ) -> Option<SubId> {
        if let Some(conn) = self.connections.get(conn_id) {
            let key = conn.add_sub(topic, group, connected_at, auto_ack);
            return Some(key);
        }

        None
    }

    pub fn remove_connection(&self, conn_id: &ConnId) -> bool {
        self.connections.remove(conn_id).is_some()
    }

    pub fn remove_sub(&self, conn_id: &ConnId, key: &SubId) -> bool {
        if let Some(conn) = self.connections.get(conn_id) {
            return conn.remove_sub(key);
        }

        false
    }
    pub fn snapshot(&self) -> serde_json::Value {
        let mut conns = Vec::new();

        for c in self.connections.iter() {
            let uptime = c.connected_at.elapsed().as_secs();
            conns.push(json!({
                "id": c.key().to_string(),
                "peer": c.peer.to_string(),
                "uptime": uptime,
                "authenticated": c.authenticated,
                "subs": c.subs.len()
            }));
        }

        serde_json::Value::Array(conns)
    }

    pub fn snapshot_subs(&self) -> serde_json::Value {
        let mut subs = Vec::new();

        for c in self.connections.iter() {
            for s in c.subs.iter() {
                subs.push(json!({
                    "conn_id": c.key().to_string(),
                    "sub_id": s.key().to_string(),
                    "topic": s.topic,
                    "group": s.group,
                    "uptime": s.connected_at.elapsed().as_secs(),
                    "auto_ack": s.auto_ack
                }));
            }
        }

        serde_json::Value::Array(subs)
    }
}
