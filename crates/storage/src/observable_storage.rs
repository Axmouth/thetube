use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::{Group, LogId, Offset, Storage, StorageError, StoredMessage, Topic};

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
    errors: AtomicU64,
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
    enabled: AtomicBool,

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
    pub fn new() -> Arc<Self> {
        let buckets = 3 * 60 * 60; // 3 hours @ 1s

        Arc::new(Self {
            enabled: AtomicBool::new(true),
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

    #[inline(always)]
    pub fn enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }
}

macro_rules! observe {
    ($stats:expr, $field:ident, $call:expr) => {{
        if !$stats.enabled() {
            $call.await
        } else {
            let _t = Timer::new(&$stats.$field.latency);
            let res = $call.await;
            $stats.$field.incr();
            if res.is_err() {
                $stats.$field.errors.fetch_add(1, Ordering::Relaxed);
            }
            res
        }
    }};
}

#[derive(Debug)]
pub struct ObservableStorage<S> {
    inner: S,
    stats: Arc<StorageStats>,
}

impl<S> ObservableStorage<S> {
    pub fn new(inner: S, stats: Arc<StorageStats>) -> Self {
        Self { inner, stats }
    }

    #[inline(always)]
    fn inner(&self) -> &S {
        &self.inner
    }

    pub fn stats(&self) -> Arc<StorageStats> {
        Arc::clone(&self.stats)
    }
}

#[async_trait::async_trait]
impl<S: Storage> Storage for ObservableStorage<S> {
    async fn append(
        &self,
        topic: &Topic,
        partition: LogId,
        payload: &[u8],
    ) -> Result<Offset, StorageError> {
        observe!(
            self.stats,
            writes,
            self.inner().append(topic, partition, payload)
        )
    }

    async fn append_batch(
        &self,
        topic: &Topic,
        partition: LogId,
        payloads: &[Vec<u8>],
    ) -> Result<Vec<Offset>, StorageError> {
        let bytes: usize = payloads.iter().map(|p| p.len()).sum();
        self.stats
            .append_batches
            .observe(payloads.len(), bytes);

        let _t = Timer::new(&self.stats.append_batches.batches.latency);
        self.inner()
            .append_batch(topic, partition, payloads)
            .await
    }

    async fn fetch_by_offset(
        &self,
        topic: &Topic,
        partition: LogId,
        offset: Offset,
    ) -> Result<StoredMessage, StorageError> {
        observe!(
            self.stats,
            reads,
            self.inner().fetch_by_offset(topic, partition, offset)
        )
    }

    async fn ack(
        &self,
        topic: &Topic,
        partition: LogId,
        group: &Group,
        offset: Offset,
    ) -> Result<(), StorageError> {
        observe!(
            self.stats,
            acks,
            self.inner().ack(topic, partition, group, offset)
        )
    }

    async fn ack_batch(
        &self,
        topic: &Topic,
        partition: LogId,
        group: &Group,
        offsets: &[Offset],
    ) -> Result<(), StorageError> {
        self.stats.ack_batches.observe(offsets.len(), 0);

        let _t = Timer::new(&self.stats.ack_batches.batches.latency);
        self.inner()
            .ack_batch(topic, partition, group, offsets)
            .await
    }

    async fn flush(&self) -> Result<(), StorageError> {
        observe!(self.stats, maintenance, self.inner().flush())
    }

    async fn register_group(
        &self,
        topic: &Topic,
        partition: LogId,
        group: &Group,
    ) -> Result<(), StorageError> {
        observe!(
            self.stats,
            control_plane,
            self.inner().register_group(topic, partition, group)
        )
    }

    async fn fetch_available(
        &self,
        topic: &Topic,
        partition: LogId,
        group: &Group,
        from_offset: Offset,
        max: usize,
    ) -> Result<Vec<crate::DeliverableMessage>, StorageError> {
        observe!(
            self.stats,
            reads,
            self.inner()
                .fetch_available(topic, partition, group, from_offset, max)
        )
    }

    async fn current_next_offset(
        &self,
        topic: &Topic,
        partition: LogId,
    ) -> Result<Offset, StorageError> {
        observe!(
            self.stats,
            reads,
            self.inner().current_next_offset(topic, partition)
        )
    }

    async fn fetch_available_clamped(
        &self,
        topic: &Topic,
        partition: LogId,
        group: &Group,
        from_offset: Offset,
        max_offset_exclusive: Offset,
        max_batch: usize,
    ) -> Result<Vec<crate::DeliverableMessage>, StorageError> {
        observe!(
            self.stats,
            reads,
            self.inner().fetch_available_clamped(
                topic,
                partition,
                group,
                from_offset,
                max_offset_exclusive,
                max_batch
            )
        )
    }

    async fn fetch_range(
        &self,
        topic: &Topic,
        partition: LogId,
        group: &Group,
        from_offset: Offset,
        to_offset: Offset,
        max_batch: usize,
    ) -> Result<Vec<crate::DeliverableMessage>, StorageError> {
        observe!(
            self.stats,
            reads,
            self.inner()
                .fetch_range(topic, partition, group, from_offset, to_offset, max_batch)
        )
    }

    async fn mark_inflight(
        &self,
        topic: &Topic,
        partition: LogId,
        group: &Group,
        offset: Offset,
        deadline_ts: crate::UnixMillis,
    ) -> Result<(), StorageError> {
        observe!(
            self.stats,
            inflight,
            self.inner()
                .mark_inflight(topic, partition, group, offset, deadline_ts)
        )
    }

    async fn mark_inflight_batch(
        &self,
        topic: &Topic,
        partition: LogId,
        group: &Group,
        entries: &[(Offset, crate::UnixMillis)],
    ) -> Result<(), StorageError> {
        observe!(
            self.stats,
            inflight,
            self.inner()
                .mark_inflight_batch(topic, partition, group, entries)
        )
    }

    async fn list_expired(&self, now_ts: crate::UnixMillis) -> Result<Vec<crate::DeliverableMessage>, StorageError> {
        observe!(
            self.stats,
            redelivery,
            self.inner().list_expired(now_ts)
        )
    }

    async fn lowest_unacked_offset(
        &self,
        topic: &Topic,
        partition: LogId,
        group: &Group,
    ) -> Result<Offset, StorageError> {
        observe!(
            self.stats,
            reads,
            self.inner().lowest_unacked_offset(topic, partition, group)
        )
    }

    async fn cleanup_topic(&self, topic: &Topic, partition: LogId) -> Result<(), StorageError> {
        observe!(
            self.stats,
            maintenance,
            self.inner().cleanup_topic(topic, partition)
        )
    }

    async fn clear_inflight(
        &self,
        topic: &Topic,
        partition: LogId,
        group: &Group,
        offset: Offset,
    ) -> Result<(), StorageError> {
        observe!(
            self.stats,
            inflight,
            self.inner().clear_inflight(topic, partition, group, offset)
        )
    }

    async fn clear_all_inflight(&self) -> Result<(), StorageError> {
        observe!(
            self.stats,
            maintenance,
            self.inner().clear_all_inflight()
        )
    }

    async fn count_inflight(
        &self,
        topic: &Topic,
        partition: LogId,
        group: &Group,
    ) -> Result<usize, StorageError> {
        observe!(
            self.stats,
            reads,
            self.inner().count_inflight(topic, partition, group)
        )
    }

    async fn is_acked(
        &self,
        topic: &Topic,
        partition: LogId,
        group: &Group,
        offset: Offset,
    ) -> Result<bool, StorageError> {
        observe!(
            self.stats,
            reads,
            self.inner().is_acked(topic, partition, group, offset)
        )
    }

    async fn is_inflight_or_acked(
        &self,
        topic: &Topic,
        partition: LogId,
        group: &Group,
        offset: Offset,
    ) -> Result<bool, StorageError> {
        observe!(
            self.stats,
            reads,
            self.inner()
                .is_inflight_or_acked(topic, partition, group, offset)
        )
    }

    async fn list_topics(&self) -> Result<Vec<Topic>, StorageError> {
        observe!(
            self.stats,
            control_plane,
            self.inner().list_topics()
        )
    }

    async fn list_groups(&self) -> Result<Vec<(Topic, LogId, Group)>, StorageError> {
        observe!(
            self.stats,
            control_plane,
            self.inner().list_groups()
        )
    }

    async fn lowest_not_acked_offset(
        &self,
        topic: &Topic,
        partition: LogId,
        group: &Group,
    ) -> Result<Offset, StorageError> {
        observe!(
            self.stats,
            reads,
            self.inner().lowest_not_acked_offset(topic, partition, group)
        )
    }

    async fn next_expiry_hint(&self) -> Result<Option<u64>, StorageError> {
        observe!(
            self.stats,
            maintenance,
            self.inner().next_expiry_hint()
        )
    }

    async fn recompute_and_store_next_expiry_hint(&self) -> Result<Option<u64>, StorageError> {
        observe!(
            self.stats,
            redelivery,
            self.inner().recompute_and_store_next_expiry_hint()
        )
    }

    async fn dump_meta_keys(&self) {
        self.inner().dump_meta_keys().await
    }
}
