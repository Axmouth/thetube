pub mod coordination;

use crossbeam::queue::SegQueue;
use dashmap::DashMap;
use std::{
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::coordination::Coordination;
use fibril_storage::{DeliverableMessage, Group, LogId, Offset, Storage, StorageError, Topic};
use fibril_util::unix_millis;

macro_rules! invariant {
    ($cond:expr, $($arg:tt)*) => {
        if cfg!(debug_assertions) && !$cond {
            panic!($($arg)*);
        }
    };
}

#[derive(thiserror::Error, Debug)]
pub enum BrokerError {
    #[error("not the leader for topic {0} partition {1}")]
    NotLeader(String, u32),

    #[error("storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("coordination error: {0}")]
    Coordination(String),

    #[error("anyhow error: {0}")]
    Anyhow(#[from] anyhow::Error),

    #[error("channel closed")]
    ChannelClosed,

    #[error("batch append failed: {0}")]
    BatchAppendFailed(String),

    #[error("unknown error: {0}")]
    Unknown(String),
}

type ConsumerId = u64;

#[derive(Debug)]
struct GroupCursor {
    pub next_offset: u64,
}

#[derive(Debug)]
struct GroupState {
    consumers: DashMap<ConsumerId, mpsc::Sender<DeliverableMessage>>,
    next_offset: AtomicU64,
    delivery_task_started: AtomicBool,
    rr_counter: AtomicU64,
    redelivery: Arc<SegQueue<Offset>>, // lock-free FIFO queue
    pending_delivery: SegQueue<DeliverableMessage>,
    pending_inflight: SegQueue<(Offset, u64)>, // offset + deadline
    inflight_sem: Arc<Semaphore>,
    inflight_permits: DashMap<Offset, OwnedSemaphorePermit>,
    notify: Arc<Notify>,
}

impl GroupState {
    fn new() -> Self {
        Self {
            consumers: DashMap::new(),
            next_offset: AtomicU64::new(0),
            delivery_task_started: AtomicBool::new(false),
            rr_counter: AtomicU64::new(0),
            redelivery: Arc::new(SegQueue::new()),
            pending_delivery: SegQueue::new(),
            pending_inflight: SegQueue::new(),
            inflight_sem: Arc::new(Semaphore::new(0)),
            inflight_permits: DashMap::new(),
            notify: Arc::new(Notify::new()),
        }
    }
}

pub struct AckRequest {
    // pub group: Group,
    // pub topic: Topic,
    // pub partition: Partition,
    pub delivery_tag: Offset,
}

pub struct PublishRequest {
    pub payload: Vec<u8>,
    pub reply: oneshot::Sender<Result<Offset, BrokerError>>,
}

#[derive(Debug, Clone)]
pub struct BrokerConfig {
    pub cleanup_interval_secs: u64,
    pub inflight_ttl_secs: u64, // seconds
    pub publish_batch_size: usize,
    pub publish_batch_timeout_ms: u64,
    pub ack_batch_size: usize,
    pub ack_batch_timeout_ms: u64,
    /// Unsafe startup option to reset all inflight state
    pub reset_inflight: bool,
}

impl Default for BrokerConfig {
    fn default() -> Self {
        BrokerConfig {
            cleanup_interval_secs: 60,
            inflight_ttl_secs: 60,
            publish_batch_size: 128,
            publish_batch_timeout_ms: 50,
            ack_batch_size: 512,
            ack_batch_timeout_ms: 2,
            reset_inflight: false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ConsumerConfig {
    pub prefetch_count: usize,
}

impl Default for ConsumerConfig {
    fn default() -> Self {
        Self { prefetch_count: 32 }
    }
}

impl ConsumerConfig {
    pub fn with_prefetch_count(self, prefetch_count: usize) -> Self {
        Self { prefetch_count }
    }
}

// TODO: Use request IDs?
// TODO: empty and close channels on drop/shutdown
pub struct ConsumerHandle {
    pub sub_id: u64,
    pub config: ConsumerConfig,
    pub group: Group,
    pub topic: Topic,
    pub partition: LogId,
    // TODO: find way to make this complete not on channel deliver, but response sent
    pub messages: tokio::sync::mpsc::Receiver<DeliverableMessage>,
    // TODO: Should it be ack only or generally respond?
    pub acker: tokio::sync::mpsc::Sender<AckRequest>,
}

impl ConsumerHandle {
    pub async fn ack(&self, ack_request: AckRequest) -> Result<(), BrokerError> {
        self.acker
            .send(ack_request)
            .await
            .map_err(|_| BrokerError::ChannelClosed)
    }

    pub async fn recv(&mut self) -> Option<DeliverableMessage> {
        self.messages.recv().await
    }
}

// TODO: Use request IDs?
// TODO: empty and close channels on drop/shutdown
pub struct PublisherHandle {
    pub publisher: tokio::sync::mpsc::Sender<PublishRequest>,
    // TODO: separate?
    confirm_tx: tokio::sync::mpsc::Sender<Result<Offset, BrokerError>>,
    task_group: Arc<TaskGroup>,
}

// TODO: empty and close channels on drop/shutdown
pub struct ConfirmStream {
    rx: mpsc::Receiver<Result<Offset, BrokerError>>,
}

impl PublisherHandle {
    pub async fn publish(&self, payload: Vec<u8>) -> Result<(), BrokerError> {
        let (tx, rx) = oneshot::channel();

        self.publisher
            .send(PublishRequest { payload, reply: tx })
            .await
            .map_err(|_| BrokerError::ChannelClosed)?;

        let confirm_rx = self.confirm_tx.clone();
        self.task_group.spawn(async move {
            if let Ok(Ok(offset)) = rx.await {
                let _ = confirm_rx.send(Ok(offset)).await;
            } else {
                let _ = confirm_rx.send(Err(BrokerError::ChannelClosed)).await;
            }
        });

        Ok(())
    }

    pub async fn shutdown(&self) {
        self.task_group.shutdown().await;
    }
}

impl ConfirmStream {
    pub async fn recv_confirm(&mut self) -> Option<Result<Offset, BrokerError>> {
        self.rx.recv().await
    }
}

#[derive(Debug)]
struct TaskGroup {
    handles: SegQueue<tokio::task::JoinHandle<()>>,
    shutdown: AtomicBool,
}

impl TaskGroup {
    fn new() -> Self {
        Self {
            handles: SegQueue::new(),
            shutdown: AtomicBool::new(false),
        }
    }

    fn spawn<F>(&self, fut: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        // Hard gate: no tasks after shutdown
        if self.shutdown.load(Ordering::Acquire) {
            return;
        }

        let handle = tokio::spawn(fut);

        // Push only if still open
        if self.shutdown.load(Ordering::Acquire) {
            handle.abort(); // defensive, extremely rare
        } else {
            self.handles.push(handle);
        }
    }

    async fn shutdown(&self) {
        // Close the gate
        self.shutdown.store(true, Ordering::Release);

        // Drain deterministically
        while let Some(h) = self.handles.pop() {
            h.abort();
        }
    }
}

// TODO: Injectable clock for testing
#[derive(Debug)]
pub struct Broker<C: Coordination + Send + Sync + 'static> {
    pub config: BrokerConfig,
    storage: Arc<dyn Storage>,
    coord: Arc<C>,
    // TODO: Add partition to groups key for corrected and independent cursors
    groups: Arc<DashMap<(Topic, Group), Arc<GroupState>>>,
    topics: Arc<DashMap<Topic, ()>>,
    batchers: Arc<DashMap<(Topic, LogId), mpsc::Sender<PublishRequest>>>,
    shutdown: CancellationToken,
    task_group: Arc<TaskGroup>,
    recovered_cursors: Arc<DashMap<(Topic, LogId, Group), GroupCursor>>,
    next_sub_id: AtomicU64,
}

impl<C: Coordination + Send + Sync + 'static> Broker<C> {
    pub async fn try_new(
        storage: impl Storage + 'static,
        coord: C,
        config: BrokerConfig,
    ) -> Result<Self, BrokerError> {
        let storage = Arc::new(storage);
        let shutdown = CancellationToken::new();

        if config.reset_inflight {
            storage.clear_all_inflight().await?;
        }

        let now_unix_ts = unix_millis();
        let expired = storage.list_expired(now_unix_ts).await?;

        // Clear expired inflight entries on startup, to avoid stuck messages
        // Rest can be redelivered after TTL by redelivery worker
        for msg in expired {
            storage
                .clear_inflight(
                    &msg.message.topic,
                    msg.message.partition,
                    &msg.group,
                    msg.message.offset,
                )
                .await?;
        }

        // Reconstructing cursor
        let groups = storage.list_groups().await?;
        let recovered_cursors = Arc::new(DashMap::new());

        for (topic, partition, group) in groups {
            let lowest = storage
                .lowest_unacked_offset(&topic, partition, &group)
                .await?;
            let cur = compute_start_offset(&storage, &topic, partition, &group, lowest).await?;

            debug_assert!(
                !storage
                    .is_inflight_or_acked(&topic, partition, &group, cur)
                    .await?,
                "recovered cursor points at inflight/acked offset"
            );

            recovered_cursors.insert((topic, partition, group), GroupCursor { next_offset: cur });
        }

        let broker = Broker {
            config,
            storage,
            coord: Arc::new(coord),
            groups: Arc::new(DashMap::new()),
            topics: Arc::new(DashMap::new()),
            batchers: Arc::new(DashMap::new()),
            shutdown,
            task_group: Arc::new(TaskGroup::new()),
            recovered_cursors,
            next_sub_id: AtomicU64::new(0),
        };

        broker.start_redelivery_worker();
        broker.start_cleanup_worker();

        Ok(broker)
    }

    pub async fn debug_upper(&self, topic: &str, partition: u32) -> u64 {
        self.storage
            .current_next_offset(&topic.to_string(), partition)
            .await
            .unwrap_or(0)
    }

    pub async fn dump_meta_keys(&self) {
        self.storage.dump_meta_keys().await;
    }

    pub async fn flush_storage(&self) -> Result<(), BrokerError> {
        self.storage.flush().await?;
        Ok(())
    }

    pub async fn forced_cleanup(&self, topic: &Topic, partition: LogId) -> Result<(), BrokerError> {
        self.storage.cleanup_topic(topic, partition).await?;
        Ok(())
    }

    pub async fn get_publisher(
        &self,
        topic: &str,
    ) -> Result<(PublisherHandle, ConfirmStream), BrokerError> {
        let partition = 0;
        let (confirm_tx, confirm_rx) =
            mpsc::channel::<Result<Offset, BrokerError>>(self.config.publish_batch_size * 4);

        let batcher = self
            .get_or_create_batcher(&topic.to_string(), partition)
            .await;

        Ok((
            PublisherHandle {
                publisher: batcher,
                confirm_tx,
                task_group: self.task_group.clone(),
            },
            ConfirmStream { rx: confirm_rx },
        ))
    }

    pub async fn publish(&self, topic: &str, payload: &[u8]) -> Result<Offset, BrokerError> {
        let partition = 0;
        let (tx, rx) = oneshot::channel();

        let batcher = self
            .get_or_create_batcher(&topic.to_string(), partition)
            .await;

        batcher
            .send(PublishRequest {
                payload: payload.to_vec(),
                reply: tx,
            })
            .await
            .map_err(|_| BrokerError::Unknown("batcher closed".into()))?;

        let offset = rx
            .await
            .map_err(|_| BrokerError::Unknown("batcher died".into()))??;

        Ok(offset)
    }

    pub async fn subscribe(
        &self,
        topic: &str,
        group: &str,
        cfg: ConsumerConfig,
    ) -> Result<ConsumerHandle, BrokerError> {
        let prefetch_count = if cfg.prefetch_count == 0 {
            1024
        } else {
            cfg.prefetch_count
        };
        let sub_id = self.next_sub_id.fetch_add(1, Ordering::SeqCst);
        self.topics.entry(topic.to_string()).or_insert(());

        self.storage
            .register_group(&topic.to_string(), 0, &group.to_string())
            .await?;

        let topic_clone = topic.to_string();
        let group_clone = group.to_string();
        let key = (topic_clone.clone(), group_clone.clone());

        let (msg_tx, msg_rx) = mpsc::channel(prefetch_count);
        let (ack_tx, mut ack_rx) = mpsc::channel::<AckRequest>(prefetch_count);
        let ack_tx_clone = ack_tx.clone();

        // Register consumer in group state
        let entry = self.groups.entry(key.clone());

        let partition = 0;
        let group_state_arc = match entry {
            dashmap::mapref::entry::Entry::Occupied(e) => e.get().clone(),
            dashmap::mapref::entry::Entry::Vacant(v) => {
                let gs = Arc::new(GroupState {
                    inflight_sem: Arc::new(Semaphore::new(prefetch_count)),
                    ..GroupState::new()
                });

                if let Some((_key, cursor)) = self.recovered_cursors.remove(&(
                    topic_clone.clone(),
                    partition,
                    group_clone.clone(),
                )) {
                    gs.next_offset.store(cursor.next_offset, Ordering::SeqCst);
                } else {
                    // brand-new group => start from 0 (or retention floor)
                    let cur = compute_start_offset(
                        &self.storage,
                        &topic_clone,
                        partition,
                        &group_clone,
                        0,
                    )
                    .await?;
                    gs.next_offset.store(cur, Ordering::SeqCst);
                }
                v.insert(gs).clone()
            }
        };

        let consumer_id = group_state_arc.rr_counter.fetch_add(1, Ordering::SeqCst);
        group_state_arc
            .consumers
            .insert(consumer_id, msg_tx.clone());
        group_state_arc.notify.notify_one();

        // Start delivery task once per (topic, group)
        if !group_state_arc
            .delivery_task_started
            .swap(true, Ordering::SeqCst)
        {
            let storage = Arc::clone(&self.storage);
            let coord = self.coord.clone();
            let groups = self.groups.clone();

            let ttl = self.config.inflight_ttl_secs;
            let shutdown = self.shutdown.clone();

            self.task_group.spawn(async move {
                let mut inflight_batch = Vec::with_capacity(32);
                // TODO: Keep track of still unacked messages, redeliver on consumer disconnect
                // Dedicated delivery loop for this (topic, group)
                loop {
                    if shutdown.is_cancelled() {
                        break;
                    }

                    // ⚠️ DO NOT add storage scans here. This loop must stay O(1).

                    // Lookup group_state each iteration so we see updated consumers
                    let group_state_opt = groups.get(&key);
                    let group_state = match group_state_opt {
                        Some(g) => g.value().clone(),
                        None => {
                            // Group removed; nothing to do
                            break;
                        }
                    };

                    let partition = 0;

                    // TODO: replace with leadership watch / notification
                    if !coord.is_leader(topic_clone.clone(), partition).await {
                        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                        continue;
                    }

                    // Snapshot consumer list
                    let consumers: Vec<(ConsumerId, mpsc::Sender<DeliverableMessage>)> =
                        group_state
                            .consumers
                            .iter()
                            .map(|entry| (*entry.key(), entry.value().clone()))
                            .collect();

                    if consumers.is_empty() {
                        group_state.notify.notified().await;
                        continue;
                    }

                    let mut delivered_any_redelivery = false;

                    // 1. Deliver all expired messages first
                    while let Some(expired_off) = group_state.redelivery.pop() {
                        // fetch the message directly by offset
                        if let Ok(msg) = storage.fetch_by_offset(&topic_clone, 0, expired_off).await
                        {
                            // reinsert inflight entry with new deadline
                            let new_deadline = unix_millis() + ttl * 1000;

                            // TODO: Improve to account for dead consumers
                            // deliver to consumer via round-robin
                            let rr = group_state.rr_counter.fetch_add(1, Ordering::SeqCst) as usize;

                            // TODO: handle empty consumers case (though ironic case to happen in delivery loop)
                            let (cid, tx) = &consumers[rr % consumers.len()];

                            let permit: OwnedSemaphorePermit =
                                match group_state.inflight_sem.clone().acquire_owned().await {
                                    Ok(p) => p,
                                    Err(_) => break, // no capacity -> stop redelivery
                                };
                            if tx
                                .send(DeliverableMessage {
                                    message: msg.clone(),
                                    delivery_tag: expired_off,
                                    group: group_clone.clone(),
                                })
                                .await
                                .is_err()
                            {
                                drop(permit);
                                group_state.consumers.remove(cid);
                                group_state.notify.notify_one();
                            } else {
                                inflight_batch.push((expired_off, new_deadline, permit));
                                delivered_any_redelivery = true;
                            }

                            // IMPORTANT: DO NOT TOUCH next_offset HERE
                        }
                    }

                    if delivered_any_redelivery && !inflight_batch.is_empty() {
                        let res = storage
                            .mark_inflight_batch(
                                &topic_clone,
                                0,
                                &group_clone,
                                &inflight_batch
                                    .iter()
                                    .map(|(e, d, _)| (*e, *d))
                                    .collect::<Vec<_>>(),
                            )
                            .await;

                        if res.is_err() {
                            drop(inflight_batch.drain(..));
                            // durability failed -> retry later
                            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                            continue;
                        } else {
                            // inflight is now durable -> consume capacity
                            for (offset, _, permit) in inflight_batch.drain(..) {
                                group_state.inflight_permits.insert(offset, permit);
                            }
                        }

                        inflight_batch.clear();
                    }

                    // IMPORTANT INVARIANT:
                    //
                    // Redeliveries always have priority over fresh messages.
                    //
                    // If we delivered *any* expired message in this iteration, we must NOT
                    // deliver fresh messages yet. This prevents newer offsets from overtaking
                    // older ones that were previously inflight and expired.
                    //
                    // Even if fresh messages are already buffered from fetch_available_clamped(),
                    // we intentionally drop them and retry in the next loop iteration, after
                    // all pending redeliveries are drained.
                    //
                    // Cursor advancement is safe because:
                    // - next_offset is only advanced after durable inflight writes
                    // - redeliveries never advance the cursor
                    if delivered_any_redelivery {
                        // We delivered retries this iteration.
                        // Do NOT deliver fresh messages yet.
                        continue;
                    }

                    let start_offset = group_state.next_offset.load(Ordering::SeqCst);

                    let partition = 0;
                    let upper = match storage.current_next_offset(&topic_clone, partition).await {
                        Ok(v) => v,
                        Err(err) => {
                            eprintln!("Error: {}", err);
                            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                            continue;
                        }
                    };

                    while group_state.inflight_sem.available_permits() == 0 {
                        group_state.notify.notified().await;
                    }
                    let available = group_state.inflight_sem.available_permits();

                    // If anything became eligible for redelivery, do not fetch fresh.
                    if !group_state.redelivery.is_empty() {
                        continue;
                    }

                    let msgs = match storage
                        .fetch_available_clamped(
                            &topic_clone,
                            partition,
                            &group_clone,
                            start_offset,
                            upper,
                            available,
                        )
                        .await
                    {
                        Ok(v) => v,
                        Err(err) => {
                            eprintln!("Error: {}", err);
                            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                            continue;
                        }
                    };

                    // do NOT fast-forward cursor if redelivery is pending
                    if !group_state.redelivery.is_empty() {
                        continue;
                    }

                    if msgs.is_empty() {
                        // No messages available *right now*.
                        // Wait until something changes (publish, ack, or redelivery).

                        group_state.notify.notified().await;
                        continue;
                    }

                    let mut max_delivered: Option<u64> = None;
                    for msg in msgs {
                        let off = msg.delivery_tag;
                        let prev = group_state.next_offset.load(Ordering::SeqCst);

                        // offsets must be monotonic
                        debug_assert!(
                            off >= prev,
                            "delivery went backwards: off={} prev={}",
                            off,
                            prev
                        );

                        // never deliver something already ACKed
                        debug_assert!(
                            storage
                                .fetch_available_clamped(
                                    &topic_clone,
                                    0,
                                    &group_clone,
                                    off,
                                    off + 1,
                                    1
                                )
                                .await
                                .unwrap()
                                .len()
                                <= 1,
                            "delivered an ACKed or inflight message: off={}",
                            off
                        );

                        // Mark inflight with deadline = now + ttl
                        let deadline = unix_millis() + ttl * 1000;

                        // Choose consumer round-robin
                        let rr = group_state.rr_counter.fetch_add(1, Ordering::SeqCst) as usize;
                        let idx = rr % consumers.len();
                        let (_cid, tx) = &consumers[idx];

                        invariant!(
                            msg.delivery_tag
                                >= group_state
                                    .next_offset
                                    .load(Ordering::SeqCst)
                                    .saturating_sub(1),
                            "delivering message behind cursor"
                        );

                        // we're in the "fresh fetch" path (msgs from fetch_available_clamped)
                        debug_assert!(
                            !delivered_any_redelivery,
                            "fresh delivery in same iteration as redelivery"
                        );

                        // Prevent sending any fresh if redelivery appears mid-iteration
                        if !group_state.redelivery.is_empty() {
                            break; // drop msgs, go to next loop iteration
                        }

                        let permit = match group_state.inflight_sem.clone().acquire_owned().await {
                            Ok(p) => p,
                            Err(_) => break, // no capacity, stop delivering
                        };

                        // Try deliver
                        if tx.send(msg).await.is_err() {
                            drop(permit); // return capacity
                            // consumer dropped; remove and DO NOT advance cursor
                            group_state.consumers.remove(&consumers[idx].0);
                            group_state.notify.notify_one();
                            break; // break out so we re-snapshot consumers next loop
                        }

                        // Only mark inflight AFTER we know message is in consumer channel
                        inflight_batch.push((off, deadline, permit));
                        max_delivered = Some(off);
                    }

                    let partition = 0;
                    if !inflight_batch.is_empty() {
                        invariant!(
                            inflight_batch.windows(2).all(|w| w[0].0 < w[1].0),
                            "inflight batch not strictly ordered"
                        );

                        let batch = &inflight_batch
                            .drain(..)
                            .map(|(e, d, p)| {
                                group_state.inflight_permits.insert(e, p);
                                (e, d)
                            })
                            .collect::<Vec<_>>();
                        let batch_clone = batch.clone();

                        let res = storage
                            .mark_inflight_batch(&topic_clone, partition, &group_clone, batch)
                            .await;

                        if res.is_err() {
                            for (offset, _) in batch_clone {
                                group_state.inflight_permits.remove(&offset);
                            }
                            // Important: do NOT advance further this iteration
                        } else {
                            // Advance cursor ONLY AFTER durability
                            if let Some(off) = max_delivered {
                                group_state.next_offset.store(off + 1, Ordering::SeqCst);
                            }

                            debug_assert!(
                                storage
                                    .is_inflight_or_acked(
                                        &topic_clone,
                                        partition,
                                        &group_clone,
                                        group_state.next_offset.load(Ordering::SeqCst) - 1
                                    )
                                    .await
                                    .unwrap_or(false),
                                "cursor advanced past durable inflight state"
                            );
                        }

                        inflight_batch.clear();
                    }
                }

                println!("Disconnect, Ending Sub");
            });
        }

        // Ack handler for this consumer
        let storage_clone = Arc::clone(&self.storage);
        let topic_clone = topic.to_string();
        let group_clone = group.to_string();

        let shutdown = self.shutdown.clone();
        let ack_batch_size = self.config.ack_batch_size;
        let ack_batch_timeout_ms = self.config.ack_batch_timeout_ms;

        let group_state = group_state_arc.clone();

        self.task_group.spawn(async move {
            let mut buf: Vec<Offset> = Vec::with_capacity(ack_batch_size);
            let mut tick =
                tokio::time::interval(std::time::Duration::from_millis(ack_batch_timeout_ms));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

            let sem = group_state_arc.inflight_sem.clone();
            loop {
                tokio::select! {
                    biased;

                    _ = shutdown.cancelled() => {
                        // Drain channel, flush once.
                        while let Ok(off) = ack_rx.try_recv() {
                            buf.push(off.delivery_tag);
                            if buf.len() >= ack_batch_size {
                                let n = buf.len();
                                let offsets = buf.clone();
                                flush_ack_batch(&storage_clone, &topic_clone, &group_clone, &mut buf).await;
                                for offset in offsets {
                                    if let Some((_, permit)) = group_state_arc.inflight_permits.remove(&offset) {
                                        drop(permit);
                                    }
                                }
                                sem.add_permits(n);
                                group_state_arc.notify.notify_one();
                            }
                        }
                        flush_ack_batch(&storage_clone, &topic_clone, &group_clone, &mut buf).await;
                        break;
                    }

                    Some(off) = ack_rx.recv() => {
                        buf.push(off.delivery_tag);
                        if buf.len() >= ack_batch_size {
                            let n = buf.len();
                            let offets = buf.clone();
                            flush_ack_batch(&storage_clone, &topic_clone, &group_clone, &mut buf).await;
                            for offset in offets {
                                if let Some((_, permit)) = group_state_arc.inflight_permits.remove(&offset) {
                                    drop(permit);
                                }
                            }
                            sem.add_permits(n);
                            group_state_arc.notify.notify_one();
                        }
                    }

                    _ = tick.tick() => {
                        let n = buf.len();
                        let offets = buf.clone();
                        flush_ack_batch(&storage_clone, &topic_clone, &group_clone, &mut buf).await;
                        for offset in offets {
                            if let Some((_, permit)) = group_state_arc.inflight_permits.remove(&offset) {
                                drop(permit);
                            }
                        }
                        sem.add_permits(n);
                        group_state_arc.notify.notify_one();
                    }
                }
            }
        });

        group_state.notify.notify_one();

        Ok(ConsumerHandle {
            sub_id,
            group: group.to_string(),
            topic: topic.to_string(),
            partition,
            config: cfg,
            messages: msg_rx,
            acker: ack_tx_clone,
        })
    }

    fn start_cleanup_worker(&self) {
        let topics = self.topics.clone();
        // TODO: List partitions too
        let storage = self.storage.clone();
        let cleanup_interval_secs = self.config.cleanup_interval_secs;
        let shutdown = self.shutdown.clone();
        self.task_group.spawn(async move {
            loop {
                if shutdown.is_cancelled() {
                    break;
                }

                tokio::time::sleep(std::time::Duration::from_secs(cleanup_interval_secs)).await;

                for topic in topics.iter() {
                    // TODO: Handle partition better
                    if let Err(err) = storage.cleanup_topic(&topic.key().to_string(), 0).await {
                        eprintln!("Error in cleanup worker: {}", err);
                    } else {
                        // TODO: Use logging (set level to warn for benches)
                        // println!("Successfully cleaned up: {}", &topic.key())
                    }
                }
            }
        });
    }

    fn start_redelivery_worker(&self) {
        let storage = Arc::clone(&self.storage);
        let coord = self.coord.clone();

        let groups = self.groups.clone();
        let shutdown = self.shutdown.clone();
        self.task_group.spawn(async move {
            loop {
                if shutdown.is_cancelled() {
                    break;
                }

                //read hint; if none, just sleep a bit or await notify
                let hint = match storage.next_expiry_hint().await {
                    Ok(v) => v,
                    Err(err) => {
                        eprintln!("Error: {}", err);
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                        continue;
                    }
                };

                if let Some(ts) = hint {
                    let now = unix_millis();
                    if ts > now {
                        tokio::time::sleep(std::time::Duration::from_millis(ts - now)).await;
                        continue;
                    }
                } else {
                    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                    continue;
                }

                // (we believe something may be expired now -> scan expired + process
                let now = unix_millis();
                let expired = match storage.list_expired(now).await {
                    Ok(v) => v,
                    Err(err) => {
                        eprintln!("Error: {}", err);
                        tokio::time::sleep(std::time::Duration::from_millis(15)).await;
                        continue;
                    }
                };

                for msg in expired {
                    if !coord
                        .is_leader(msg.message.topic.clone(), msg.message.partition)
                        .await
                    {
                        continue;
                    }

                    // TODO adjust to handle more than one expired message per group
                    if let Some(gs) = groups.get(&(msg.message.topic.clone(), msg.group.clone())) {
                        let current = gs.next_offset.load(Ordering::SeqCst);

                        debug_assert!(
                            msg.message.offset < current,
                            "expired offset >= next_offset (would duplicate): off={} next={}",
                            msg.message.offset,
                            current
                        );

                        let expired_offset = msg.message.offset;

                        if expired_offset >= current {
                            continue;
                        }

                        let is_acked = storage
                            .is_acked(
                                &msg.message.topic,
                                msg.message.partition,
                                &msg.group,
                                expired_offset,
                            )
                            .await
                            .unwrap_or(true); // conservative

                        if is_acked {
                            continue; // nothing to do
                        }
                        // now we know it still matters
                        let _ = storage
                            .clear_inflight(
                                &msg.message.topic,
                                msg.message.partition,
                                &msg.group,
                                expired_offset,
                            )
                            .await;
                        if let Some((_, permit)) = gs.inflight_permits.remove(&expired_offset) {
                            drop(permit);
                        }
                        gs.redelivery.push(expired_offset);
                        gs.notify.notify_one();
                    }
                }

                // recompute hint after processing so next sleep is accurate
                match storage.recompute_and_store_next_expiry_hint().await {
                    Ok(v) => v,
                    Err(err) => {
                        eprintln!("Error: {}", err);
                        tokio::time::sleep(std::time::Duration::from_millis(15)).await;
                        continue;
                    }
                };

                // tokio::time::sleep(std::time::Duration::from_millis(3)).await;
                tokio::task::yield_now().await;
            }
        });
    }

    async fn get_or_create_batcher(
        &self,
        topic: &Topic,
        partition: LogId,
    ) -> mpsc::Sender<PublishRequest> {
        let key = (topic.clone(), partition);

        self.topics.entry(topic.clone()).or_insert(());

        match self.batchers.entry(key) {
            dashmap::Entry::Occupied(e) => e.get().clone(),
            dashmap::Entry::Vacant(v) => {
                let (tx, rx) = mpsc::channel::<PublishRequest>(self.config.publish_batch_size * 4);
                v.insert(tx.clone());
                self.spawn_batcher(topic.clone(), partition, rx);
                tx
            }
        }
    }

    // NOTE: Fast processing partly depends on confirms being consumed quickly
    fn spawn_batcher(
        &self,
        topic: Topic,
        partition: LogId,
        mut rx: mpsc::Receiver<PublishRequest>,
    ) {
        let storage = Arc::clone(&self.storage);
        let cfg = self.config.clone();
        let shutdown = self.shutdown.clone();
        let groups = self.groups.clone();

        self.task_group.spawn(async move {
            let mut pending: Vec<PublishRequest> = Vec::new();
            let mut timer: Option<Pin<Box<tokio::time::Sleep>>> = None;

            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => break,

                    maybe = rx.recv() => {
                        match maybe {
                            Some(msg) => {
                                pending.push(msg);

                                // arm timer on first message
                                if pending.len() == 1 {
                                    timer = Some(Box::pin(tokio::time::sleep(
                                        tokio::time::Duration::from_millis(cfg.publish_batch_timeout_ms)
                                    )));
                                }

                                // size-based flush
                                if pending.len() >= cfg.publish_batch_size {
                                    flush_publish_batch(&storage, groups.clone(), &topic, partition, &mut pending).await;
                                    timer = None;
                                }
                            }

                            None => {
                                // channel closed: flush remaining
                                if !pending.is_empty() {
                                    flush_publish_batch(&storage, groups.clone(), &topic, partition, &mut pending).await;
                                }
                                break;
                            }
                        }
                    }

                    // timer-based flush
                    _ = async { timer.as_mut().unwrap().as_mut().await }, if timer.is_some() => {
                        if !pending.is_empty() {
                            flush_publish_batch(&storage, groups.clone(), &topic, partition, &mut pending).await;
                        }
                        timer = None;
                    }
                }
            }
        });
    }

    pub async fn shutdown(&self) {
        // Close batchers: this causes their tasks to exit naturally
        self.batchers.clear();

        // Clear groups so delivery loops exit
        self.groups.clear();

        // Signal shutdown to background tasks
        self.shutdown.cancel();
        self.task_group.shutdown().await;

        // Give tasks time to see the closed channels & exit
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    }
}

async fn flush_publish_batch(
    storage: &Arc<dyn Storage>,
    groups: Arc<DashMap<(String, String), Arc<GroupState>>>,
    topic: &Topic,
    partition: u32,
    pending: &mut Vec<PublishRequest>,
) {
    if pending.is_empty() {
        return;
    }

    let payloads: Vec<Vec<u8>> = pending
        .iter_mut()
        .map(|r| std::mem::take(&mut r.payload))
        .collect();

    let result = storage.append_batch(topic, partition, &payloads).await;

    match result {
        Ok(offsets) => {
            for (req, off) in pending.drain(..).zip(offsets) {
                let _ = req.reply.send(Ok(off));
            }

            for entry in groups.iter() {
                let ((t, _g), gs) = entry.pair();
                if t == topic {
                    gs.notify.notify_one();
                }
            }
        }
        Err(err) => {
            // On failure, return error to all callers
            let msg = format!("batch append failed: {err}");
            // We can't cheaply clone error; create per-recipient error
            for req in pending.drain(..) {
                let _ = req
                    .reply
                    .send(Err(BrokerError::BatchAppendFailed(msg.clone())));
            }
        }
    }
}

async fn flush_ack_batch(
    storage: &Arc<dyn Storage>,
    topic: &String,
    group: &String,
    buf: &mut Vec<Offset>,
) {
    if buf.is_empty() {
        return;
    }
    // best-effort; you can log errors
    let partition = 0;
    let _ = storage.ack_batch(topic, partition, group, buf).await;
    buf.clear();
}

async fn compute_start_offset(
    storage: &Arc<impl Storage + ?Sized>,
    topic: &str,
    partition: u32,
    group: &str,
    mut cur: u64,
) -> Result<u64, StorageError> {
    let upper = storage
        .current_next_offset(&topic.to_string(), partition)
        .await?;

    while cur < upper {
        if storage
            .is_inflight_or_acked(&topic.to_string(), partition, &group.to_string(), cur)
            .await?
        {
            cur += 1;
        } else {
            break;
        }
    }
    Ok(cur)
}

pub fn maybe_auto_ack(auto_ack: bool, ack_tx: &mpsc::Sender<AckRequest>, offset: Offset) {
    if auto_ack {
        let _ = ack_tx.try_send(AckRequest {
            delivery_tag: offset,
        });
    }
}
