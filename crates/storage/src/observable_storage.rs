use fibril_metrics::*;
use fibril_util::UnixMillis;
use stroma_core::{AppendCompletion, IoError};
use std::sync::Arc;
use std::sync::atomic::Ordering;

use crate::{Group, LogId, Offset, Storage, StorageError, StoredMessage, Topic};

macro_rules! observe {
    ($stats:expr, $field:ident, $call:expr) => {{
        let _t = Timer::new(&$stats.$field.latency);
        let res = $call.await;
        $stats.$field.incr();
        if res.is_err() {
            $stats.$field.errors.fetch_add(1, Ordering::Relaxed);
        }
        res
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
    async fn append_enqueue(
        &self,
        topic: &Topic,
        partition: LogId,
        payload: &[u8],
        completion: Box<dyn AppendCompletion<IoError>>,
    ) -> Result<(), StorageError> {
        observe!(
            self.stats,
            writes,
            self.inner().append_enqueue(topic, partition, payload, completion)
        )
    }

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
        self.stats.append_batches.observe(payloads.len(), bytes);
        self.stats.record_write();

        let _t = Timer::new(&self.stats.append_batches.batches.latency);
        self.inner().append_batch(topic, partition, payloads).await
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
        self.stats.record_acks(offsets.len() as u64);
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
        entries: &[(Offset, UnixMillis)],
    ) -> Result<(), StorageError> {
        observe!(
            self.stats,
            inflight,
            self.inner()
                .mark_inflight_batch(topic, partition, group, entries)
        )
    }

    async fn list_expired(
        &self,
        now_ts: crate::UnixMillis,
    ) -> Result<Vec<crate::DeliverableMessage>, StorageError> {
        observe!(self.stats, redelivery, self.inner().list_expired(now_ts))
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
        observe!(self.stats, maintenance, self.inner().clear_all_inflight())
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
        observe!(self.stats, control_plane, self.inner().list_topics())
    }

    async fn list_groups(&self) -> Result<Vec<(Topic, LogId, Group)>, StorageError> {
        observe!(self.stats, control_plane, self.inner().list_groups())
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
            self.inner()
                .lowest_not_acked_offset(topic, partition, group)
        )
    }

    async fn next_expiry_hint(&self) -> Result<Option<u64>, StorageError> {
        observe!(self.stats, maintenance, self.inner().next_expiry_hint())
    }

    async fn recompute_and_store_next_expiry_hint(&self) -> Result<Option<u64>, StorageError> {
        observe!(
            self.stats,
            redelivery,
            self.inner().recompute_and_store_next_expiry_hint()
        )
    }

    async fn estimate_disk_used(&self) -> Result<u64, StorageError> {
        observe!(self.stats, control_plane, self.inner().estimate_disk_used())
    }

    async fn dump_meta_keys(&self) {
        self.inner().dump_meta_keys().await
    }
}
