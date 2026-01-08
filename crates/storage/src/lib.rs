pub mod observable_storage;
pub mod rocksdb_store;
pub mod stroma_store;

use async_trait::async_trait;

use fibril_util::UnixMillis;
use serde::{Deserialize, Serialize};

pub use stroma_core::{AppendCompletion, AppendResult, CompletionPair, IoError};

pub type Topic = String;
pub type LogId = u32;
pub type Offset = u64;
pub type Group = String;

#[derive(Debug, Clone)]
pub struct StoredMessage {
    pub topic: Topic,
    pub partition: LogId,
    pub offset: Offset,
    pub timestamp: u64,
    pub payload: Vec<u8>,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct DeliveryTag {
    pub epoch: u64,
}

/// Returned by poll operations: the message plus its metadata
#[derive(Debug, Clone)]
pub struct DeliverableMessage {
    pub message: StoredMessage,
    pub delivery_tag: DeliveryTag, // unique per (topic,partition)
    pub group: Group,
}

#[derive(thiserror::Error, Debug)]
pub enum StorageError {
    #[error("column family not found: {0}")]
    MissingColumnFamily(&'static str),

    #[error("rocksdb error: {0}")]
    RocksDb(#[from] rocksdb::Error),

    #[error("invalid key encoding: {0}")]
    KeyDecode(String),

    #[error("missing message for offset {offset}")]
    MessageNotFound { offset: u64 },

    #[error("unexpected internal error: {0}")]
    Internal(String),

    #[error("anyhow error: {0}")]
    Anyhow(#[from] anyhow::Error),
}

pub fn make_rocksdb_store(
    path: &str,
    sync_write: bool,
) -> Result<rocksdb_store::RocksStorage, StorageError> {
    rocksdb_store::RocksStorage::open(path, sync_write)
}

pub async fn make_stroma_store(
    path: &str,
    sync_write: bool,
) -> Result<stroma_store::StromaStorage, StorageError> {
    stroma_store::StromaStorage::open(path, sync_write).await
}

#[async_trait]
pub trait AppendReceiptExt<T> {
    async fn wait(self) -> Result<T, impl Into<StorageError>>;
}

pub struct StorageAppendReceipt<T> {
    pub result_rx: tokio::sync::oneshot::Receiver<Result<T, StorageError>>,
}

#[async_trait]
impl AppendReceiptExt<Offset> for StorageAppendReceipt<Offset> {
    async fn wait(self) -> Result<Offset, StorageError> {
        self.result_rx
            .await
            .unwrap_or_else(|_| Err(StorageError::Internal("writer dropped".into())))
    }
}

pub struct BrokerCompletion {
    tx: tokio::sync::oneshot::Sender<Result<AppendResult, IoError>>,
}

impl AppendCompletion<IoError> for BrokerCompletion {
    fn complete(self: Box<Self>, res: Result<AppendResult, IoError>) {
        let _ = self.tx.send(res);
    }
}

pub struct BrokerCompletionPair;

impl CompletionPair<IoError> for BrokerCompletionPair {
    type Receiver = tokio::sync::oneshot::Receiver<Result<AppendResult, IoError>>;

    fn pair() -> (Box<dyn AppendCompletion<IoError>>, Self::Receiver) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        (
            Box::new(BrokerCompletion { tx }),
            rx,
        )
    }
}

/// Defines the persistent storage API for a durable queue system.
#[async_trait]
pub trait Storage: Send + Sync + std::fmt::Debug
{
    /// Append a message to the end of a topic/partition log asynchronously(a receipt is returned that can be used to await batch commit).
    async fn append_enqueue(
        &self,
        topic: &Topic,
        partition: LogId,
        payload: &[u8],
        completion: Box<dyn AppendCompletion<IoError>>,
    ) -> Result<(), StorageError>;

    /// Append a message to the end of a topic/partition log.
    async fn append(
        &self,
        topic: &Topic,
        partition: LogId,
        payload: &[u8],
    ) -> Result<Offset, StorageError>;

    /// Append a batch of messages to the end of a topic/partition log.
    async fn append_batch(
        &self,
        topic: &Topic,
        partition: LogId,
        payloads: &[Vec<u8>],
    ) -> Result<Vec<Offset>, StorageError>;

    /// Register a consumer group for a topic/partition.
    async fn register_group(
        &self,
        topic: &Topic,
        partition: LogId,
        group: &Group,
    ) -> Result<(), StorageError>;

    /// Fetch a message by its exact offset.
    async fn fetch_by_offset(
        &self,
        topic: &Topic,
        partition: LogId,
        offset: Offset,
    ) -> Result<StoredMessage, StorageError>;

    /// Fetch messages starting *after* a given offset,
    /// limited to max count, excluding messages currently in-flight.
    async fn fetch_available(
        &self,
        topic: &Topic,
        partition: LogId,
        group: &Group,
        from_offset: Offset,
        max: usize,
    ) -> Result<Vec<DeliverableMessage>, StorageError>;

    /// Get the current next offset for appending new messages.
    async fn current_next_offset(
        &self,
        topic: &Topic,
        partition: LogId,
    ) -> Result<Offset, StorageError>;

    /// Fetch messages starting *after* a given offset,
    async fn fetch_available_clamped(
        &self,
        topic: &Topic,
        partition: LogId,
        group: &Group,
        from_offset: Offset,
        max_offset_exclusive: Offset,
        max_batch: usize,
    ) -> Result<Vec<DeliverableMessage>, StorageError>;

    /// Mark a message as "in-flight" for a consumer group with a deadline.
    async fn mark_inflight(
        &self,
        topic: &Topic,
        partition: LogId,
        group: &Group,
        offset: Offset,
        deadline_ts: UnixMillis,
    ) -> Result<(), StorageError>;

    /// Marks a batch of messages as "in-flight"
    async fn mark_inflight_batch(
        &self,
        topic: &Topic,
        partition: LogId,
        group: &Group,
        entries: &[(Offset, UnixMillis)], // offset -> deadline
    ) -> Result<(), StorageError>;

    /// Remove message from inflight and mark as acknowledged.
    async fn ack(
        &self,
        topic: &Topic,
        partition: LogId,
        group: &Group,
        offset: Offset,
    ) -> Result<(), StorageError>;

    /// Acknowledge a batch of messages.
    async fn ack_batch(
        &self,
        topic: &Topic,
        partition: LogId,
        group: &Group,
        offsets: &[Offset],
    ) -> Result<(), StorageError>;

    /// Return messages whose deadline expired → need redelivery.
    async fn list_expired(
        &self,
        now_ts: UnixMillis,
    ) -> Result<Vec<DeliverableMessage>, StorageError>;

    /// Get the lowest unacknowledged offset for a consumer group.
    async fn lowest_unacked_offset(
        &self,
        topic: &Topic,
        partition: LogId,
        group: &Group,
    ) -> Result<Offset, StorageError>;

    /// Cleanup fully acknowledged messages safely.
    async fn cleanup_topic(&self, topic: &Topic, partition: LogId) -> Result<(), StorageError>;

    async fn clear_inflight(
        &self,
        topic: &Topic,
        partition: LogId,
        group: &Group,
        offset: Offset,
    ) -> Result<(), StorageError>;

    async fn clear_all_inflight(&self) -> Result<(), StorageError>;

    async fn count_inflight(
        &self,
        topic: &Topic,
        partition: LogId,
        group: &Group,
    ) -> Result<usize, StorageError>;

    async fn is_acked(
        &self,
        topic: &Topic,
        partition: LogId,
        group: &Group,
        offset: Offset,
    ) -> Result<bool, StorageError>;

    async fn is_inflight_or_acked(
        &self,
        topic: &Topic,
        partition: LogId,
        group: &Group,
        offset: Offset,
    ) -> Result<bool, StorageError>;

    async fn list_topics(&self) -> Result<Vec<Topic>, StorageError>;

    async fn list_groups(&self) -> Result<Vec<(Topic, LogId, Group)>, StorageError>;

    async fn lowest_not_acked_offset(
        &self,
        topic: &Topic,
        partition: LogId,
        group: &Group,
    ) -> Result<Offset, StorageError>;

    async fn flush(&self) -> Result<(), StorageError>;

    /// Read the hint (global earliest inflight deadline).
    async fn next_expiry_hint(&self) -> Result<Option<u64>, StorageError>;

    /// Full recompute: scan inflight CF for the minimum deadline and store it.
    /// Call this from the redelivery worker after scanning/processing.
    async fn recompute_and_store_next_expiry_hint(&self) -> Result<Option<u64>, StorageError>;

    async fn estimate_disk_used(&self) -> Result<u64, StorageError>;

    async fn dump_meta_keys(&self);
}
