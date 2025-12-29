pub mod coordination;

use crossbeam::queue::SegQueue;
use dashmap::DashMap;
use fibril_metrics::BrokerStats;
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
use fibril_storage::{
    DeliverableMessage, DeliveryTag, Group, LogId, Offset, Storage, StorageError, Topic,
};
use fibril_util::{UnixMillis, unix_millis};

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
    tag_counter: AtomicU64,
    redelivery: Arc<SegQueue<Offset>>, // lock-free FIFO queue
    inflight_sem: Arc<Semaphore>,
    delivery_tag_by_offset: DashMap<Offset, DeliveryTag>,
    inflight_permits_by_tag: DashMap<DeliveryTag, (Offset, OwnedSemaphorePermit)>,
    events: Arc<Semaphore>,
}

impl GroupState {
    fn new() -> Self {
        Self {
            consumers: DashMap::new(),
            next_offset: AtomicU64::new(0),
            delivery_task_started: AtomicBool::new(false),
            rr_counter: AtomicU64::new(0),
            tag_counter: AtomicU64::new(1),
            redelivery: Arc::new(SegQueue::new()),
            inflight_sem: Arc::new(Semaphore::new(0)),
            delivery_tag_by_offset: DashMap::new(),
            inflight_permits_by_tag: DashMap::new(),
            events: Arc::new(Semaphore::new(0)),
        }
    }

    #[inline]
    fn signal(&self) {
        // "Something changed, delivery may make progress"
        self.events.add_permits(1);
    }
}

pub struct AckRequest {
    // pub group: Group,
    // pub topic: Topic,
    // pub partition: Partition,
    pub delivery_tag: DeliveryTag,
}

pub struct PublishRequest {
    pub payload: Vec<u8>,
    pub reply: oneshot::Sender<Result<Offset, BrokerError>>,
}

#[derive(Debug, Clone)]
pub struct BrokerConfig {
    pub cleanup_interval_secs: u64,
    // TODO: Rename to better decribe? Ack deadline?
    pub inflight_ttl_secs: u64, // seconds
    pub publish_batch_size: usize,
    pub publish_batch_timeout_ms: u64,
    pub ack_batch_size: usize,
    pub ack_batch_timeout_ms: u64,
    pub inflight_batch_size: usize,
    pub inflight_batch_timeout_ms: u64,
    /// Unsafe startup option to reset all inflight state
    pub reset_inflight: bool,
}

impl Default for BrokerConfig {
    fn default() -> Self {
        BrokerConfig {
            cleanup_interval_secs: 60,
            inflight_ttl_secs: 60,
            publish_batch_size: 256,
            publish_batch_timeout_ms: 10,
            ack_batch_size: 512,
            ack_batch_timeout_ms: 10,
            inflight_batch_size: 512,
            inflight_batch_timeout_ms: 1,

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

struct DeliveryCtx {
    storage: Arc<dyn Storage>,
    group_state: Arc<GroupState>,
    topic: Topic,
    group: Group,
    partition: LogId,
    consumers: Vec<(ConsumerId, mpsc::Sender<DeliverableMessage>)>,
    ttl_deadline_delta_ms: u64,
    metrics: Arc<BrokerStats>,
    broker_config: BrokerConfig,
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
    metrics: Arc<BrokerStats>,
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
        storage: Arc<impl Storage + 'static>,
        coord: C,
        metrics: Arc<BrokerStats>,
        config: BrokerConfig,
    ) -> Result<Self, BrokerError> {
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
            metrics,
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
        group_state_arc.signal();

        // Start delivery task once per (topic, group)
        if !group_state_arc
            .delivery_task_started
            .swap(true, Ordering::SeqCst)
        {
            let storage = Arc::clone(&self.storage);
            let _coord = self.coord.clone();
            let groups = self.groups.clone();

            let ttl = self.config.inflight_ttl_secs;
            let shutdown = self.shutdown.clone();

            let metrics = self.metrics.clone();
            let broker_config = self.config.clone();
            self.task_group.spawn(async move {
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
                    // if !coord.is_leader(topic_clone.clone(), partition).await {
                    //     tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                    //     continue;
                    // }

                    // Snapshot consumer list
                    let consumers: Vec<(ConsumerId, mpsc::Sender<DeliverableMessage>)> =
                        group_state
                            .consumers
                            .iter()
                            .map(|entry| (*entry.key(), entry.value().clone()))
                            .collect();

                    if consumers.is_empty() {
                        wait_for_event(&group_state).await;
                        continue;
                    }

                    let ctx = DeliveryCtx {
                        storage: storage.clone(),
                        group_state: group_state.clone(),
                        topic: topic_clone.to_owned(),
                        group: group_clone.to_owned(),
                        partition,
                        consumers: consumers.clone(),
                        ttl_deadline_delta_ms: ttl * 1000,
                        metrics: metrics.clone(),
                        broker_config: broker_config.clone(),
                    };

                    if process_redeliveries(&ctx).await {
                        continue;
                    }

                    if process_fresh_deliveries(&ctx).await {
                        continue;
                    }

                    wait_for_event(&ctx.group_state).await;
                }

                tracing::debug!("Disconnect, Ending Sub");
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

        let metrics = self.metrics.clone();
        self.task_group.spawn(async move {
            let mut buf: Vec<Offset> = Vec::with_capacity(ack_batch_size);
            let mut tick =
                tokio::time::interval(std::time::Duration::from_millis(ack_batch_timeout_ms));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let mut flush_deadline: Option<tokio::time::Instant> = None;

            loop {
                tokio::select! {
                    biased;

                    _ = shutdown.cancelled() => {
                        // flush remaining
                        if !buf.is_empty() {
                            flush_ack_batch(&storage_clone, &topic_clone, &group_clone, &mut buf, metrics.clone()).await;
                        }
                        break;
                    }

                    Some(off) = ack_rx.recv() => {
                        let tag = off.delivery_tag;

                        debug_assert!(
                            !group_state_arc.inflight_permits_by_tag.is_empty(),
                            "ACK received but no inflight messages exist"
                        );

                        // ONLY accept ACKs for messages that are actually inflight
                        if let Some((_, (offset, permit))) = group_state_arc.inflight_permits_by_tag.remove(&tag) {
                            // free capacity
                            drop(permit);
                            group_state_arc.delivery_tag_by_offset.remove(&offset);

                            // enqueue for durable ack
                            buf.push(offset);

                            // wake delivery loop (capacity changed)
                            group_state_arc.signal();

                            // start/extend the flush timer
                            flush_deadline = Some(
                                tokio::time::Instant::now()
                                    + std::time::Duration::from_millis(ack_batch_timeout_ms)
                            );

                            // size-based flush
                            if buf.len() >= ack_batch_size {
                                flush_ack_batch(
                                    &storage_clone,
                                    &topic_clone,
                                    &group_clone,
                                    &mut buf,
                                    metrics.clone(),
                                )
                                .await;
                                flush_deadline = None;
                            }
                        } else {
                            // ACK before delivery -> ignore
                            // (optional todo: metrics / trace)
                            // metrics.ack_before_delivery();
                        }
                    }

                    // idle-timeout flush
                    _ = async {
                        if let Some(dl) = flush_deadline {
                            tokio::time::sleep_until(dl).await;
                        } else {
                            futures::future::pending::<()>().await;
                        }
                    } => {
                        if !buf.is_empty() {
                            flush_ack_batch(&storage_clone, &topic_clone, &group_clone, &mut buf, metrics.clone()).await;
                        }
                        flush_deadline = None;
                    }
                }
            }
        });

        group_state.signal();

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
                        tracing::error!("Error in cleanup worker: {}", err);
                    } else {
                        tracing::info!("Successfully cleaned up: {}", &topic.key())
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
        let metrics = self.metrics.clone();
        self.task_group.spawn(async move {
            loop {
                if shutdown.is_cancelled() {
                    break;
                }

                //read hint; if none, just sleep a bit or await notify
                let hint = match storage.next_expiry_hint().await {
                    Ok(v) => v,
                    Err(err) => {
                        tracing::error!("Error: {}", err);
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
                        tracing::error!("Error: {}", err);
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
                        if let Some((_, tag)) = gs.delivery_tag_by_offset.remove(&expired_offset)
                            && let Some((_, (_off, permit))) =
                                gs.inflight_permits_by_tag.remove(&tag)
                        {
                            drop(permit);
                        }
                        gs.redelivery.push(msg.message.offset);
                        metrics.expired();
                        gs.signal();
                    }
                }

                // recompute hint after processing so next sleep is accurate
                match storage.recompute_and_store_next_expiry_hint().await {
                    Ok(v) => v,
                    Err(err) => {
                        tracing::error!("Error: {}", err);
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

        let metrics = self.metrics.clone();
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
                                    flush_publish_batch(&storage, groups.clone(), &topic, partition, &mut pending, metrics.clone()).await;
                                    timer = None;
                                }
                            }

                            None => {
                                // channel closed: flush remaining
                                if !pending.is_empty() {
                                    flush_publish_batch(&storage, groups.clone(), &topic, partition, &mut pending,  metrics.clone()).await;
                                }
                                break;
                            }
                        }
                    }

                    // timer-based flush
                    // TODO: refactor unwrap
                    _ = async { timer.as_mut().unwrap().as_mut().await }, if timer.is_some() => {
                        if !pending.is_empty() {
                            flush_publish_batch(&storage, groups.clone(), &topic, partition, &mut pending,  metrics.clone()).await;
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
    metrics: Arc<BrokerStats>,
) {
    if pending.is_empty() {
        return;
    }

    let payloads: Vec<Vec<u8>> = pending
        .iter_mut()
        .map(|r| std::mem::take(&mut r.payload))
        .collect();

    let batch_size = payloads.len();
    let bytes = payloads.iter().map(|p| p.len()).sum::<usize>();

    let result = storage.append_batch(topic, partition, &payloads).await;

    match result {
        Ok(offsets) => {
            metrics.published_many(offsets.len() as u64);
            metrics.publish_batch(batch_size, bytes);
            for (req, off) in pending.drain(..).zip(offsets) {
                let _ = req.reply.send(Ok(off));
            }

            for entry in groups.iter() {
                let ((t, _g), gs) = entry.pair();
                if t == topic {
                    gs.signal();
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
    metrics: Arc<BrokerStats>,
) {
    if buf.is_empty() {
        return;
    }
    // best-effort; you can log errors
    let partition = 0;
    let batch_size = buf.len();
    if storage
        .ack_batch(topic, partition, group, buf)
        .await
        .is_ok()
    {
        metrics.acked_many(buf.len() as u64);
        metrics.ack_batch(batch_size, batch_size);
    }
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

pub fn maybe_auto_ack(auto_ack: bool, ack_tx: &mpsc::Sender<AckRequest>, tag: DeliveryTag) {
    if auto_ack {
        let _ = ack_tx.try_send(AckRequest { delivery_tag: tag });
    }
}

async fn process_redeliveries(ctx: &DeliveryCtx) -> bool {
    let DeliveryCtx {
        storage,
        group_state,
        topic,
        group,
        consumers,
        ttl_deadline_delta_ms: ttl_ms,
        metrics,
        ..
    } = ctx;

    let mut delivered_any = false;
    let mut inflight_batch: Vec<(DeliveryTag, Offset, UnixMillis, OwnedSemaphorePermit)> =
        Vec::new();

    while let Some(expired_offset) = group_state.redelivery.pop() {
        if let Ok(msg) = storage.fetch_by_offset(topic, 0, expired_offset).await {
            let new_deadline = unix_millis() + ttl_ms;

            let rr = group_state.rr_counter.fetch_add(1, Ordering::SeqCst) as usize;
            let (cid, tx) = &consumers[rr % consumers.len()];

            let permit = match group_state.inflight_sem.clone().acquire_owned().await {
                Ok(p) => p,
                Err(_) => {
                    // no capacity; put it back and stop
                    group_state.redelivery.push(expired_offset);
                    break;
                }
            };

            let epoch = group_state.tag_counter.fetch_add(1, Ordering::SeqCst);
            let tag = DeliveryTag { epoch };
            group_state.delivery_tag_by_offset.insert(msg.offset, tag);
            if tx
                .send(DeliverableMessage {
                    message: msg,
                    delivery_tag: tag,
                    group: group.clone(),
                })
                .await
                .is_err()
            {
                drop(permit);
                group_state.consumers.remove(cid);
                group_state.signal(); // consumer set changed
                // requeue the message for someone else later
                group_state.redelivery.push(expired_offset);
            } else {
                // group_state.inflight_permits.remove(&tag);
                inflight_batch.push((tag, expired_offset, new_deadline, permit));
                delivered_any = true;
                metrics.redelivered();
                metrics.delivered();
            }
        }
    }

    if inflight_batch.is_empty() {
        return delivered_any;
    }

    let res = storage
        .mark_inflight_batch(
            topic,
            0,
            group,
            &inflight_batch
                .iter()
                .map(|(_t, o, d, _p)| (*o, *d))
                .collect::<Vec<_>>(),
        )
        .await;

    if res.is_err() {
        // Not durable: release capacity and requeue offsets
        for (tag, offset, _deadline, permit) in inflight_batch.drain(..) {
            group_state.inflight_permits_by_tag.remove(&tag);
            drop(permit);
            group_state.redelivery.push(offset);
        }
        // make sure we run again promptly
        group_state.signal();
        // optional tiny backoff if storage is dying
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        return true;
    }

    // Durable: keep permits held by inflight_permits
    for (tag, offset, _deadline, permit) in inflight_batch.drain(..) {
        // TODO: make sure it happened earlier as much as possible, eval whether we need to replace tags
        group_state
            .inflight_permits_by_tag
            .insert(tag, (offset, permit));
        group_state.delivery_tag_by_offset.insert(offset, tag);
    }

    delivered_any
}

async fn process_fresh_deliveries(ctx: &DeliveryCtx) -> bool {
    let DeliveryCtx {
        storage,
        group_state,
        topic,
        group,
        consumers,
        ttl_deadline_delta_ms,
        metrics,
        ..
    } = ctx;

    // capacity gate
    let available = group_state
        .inflight_sem
        .available_permits()
        .min(ctx.broker_config.inflight_batch_size);
    if available == 0 || !group_state.redelivery.is_empty() {
        return false;
    }

    let start = group_state.next_offset.load(Ordering::SeqCst);
    let upper = match storage.current_next_offset(topic, 0).await {
        Ok(v) => v,
        Err(_) => return false,
    };

    let mut msgs = match storage
        .fetch_available_clamped(topic, 0, group, start, upper, available)
        .await
    {
        Ok(v) if !v.is_empty() => v,
        _ => return false,
    };

    // ---- phase 1: acquire permits + build inflight batch
    let mut entries = Vec::with_capacity(msgs.len());
    let mut permits = Vec::with_capacity(msgs.len());

    for msg in &mut msgs {
        match group_state.inflight_sem.clone().acquire_owned().await {
            Ok(p) => {
                let epoch = group_state.tag_counter.fetch_add(1, Ordering::SeqCst);
                msg.delivery_tag.epoch = epoch;
                group_state
                    .delivery_tag_by_offset
                    .insert(msg.message.offset, msg.delivery_tag);
                permits.push((msg.delivery_tag, p));
                entries.push((msg.message.offset, unix_millis() + ttl_deadline_delta_ms));
            }
            Err(_) => break,
        }
    }

    if entries.is_empty() {
        return false;
    }

    // ---- phase 2: durable inflight
    if storage
        .mark_inflight_batch(topic, 0, group, &entries)
        .await
        .is_err()
    {
        // release permits
        for (_, p) in permits {
            drop(p);
        }
        return false;
    }

    // ---- phase 3: deliver + register permits
    let mut max_off = None;

    for ((tag, permit), msg) in permits.into_iter().zip(msgs.into_iter()) {
        let rr = group_state.rr_counter.fetch_add(1, Ordering::SeqCst) as usize;
        let idx = rr % consumers.len();
        let (_cid, tx) = &consumers[idx];

        let offset = msg.message.offset;
        if tx.send(msg).await.is_err() {
            drop(permit);
            group_state.consumers.remove(&consumers[idx].0);
            group_state.signal();
            // best effort: clear inflight later via expiry
            continue;
        }

        group_state
            .inflight_permits_by_tag
            .insert(tag, (offset, permit));
        max_off = Some(offset);
        metrics.delivered();
    }

    if let Some(off) = max_off {
        group_state.next_offset.store(off + 1, Ordering::SeqCst);
    }

    true
}

#[inline]
async fn wait_for_event(group_state: &GroupState) {
    // wait until at least one event happens
    let permit = group_state.events.acquire().await;
    if let Ok(p) = permit {
        p.forget(); // consume exactly 1 event
    }
}
