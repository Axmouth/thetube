use fibril_storage::DeliveryTag;
use futures::{SinkExt, StreamExt};
use serde::de::DeserializeOwned;
use std::{collections::HashMap, sync::Arc};
use tokio::{
    net::TcpStream,
    sync::{
        Mutex, Notify,
        mpsc::{self, Receiver},
        oneshot,
    },
};
use tokio_util::codec::Framed;

use fibril_protocol::v1::{frame::ProtoCodec, helper::*, *};

// TODO: custom error, this error

// ===== Public API ============================================================

pub struct Client {
    engine: Arc<EngineHandle>,
}

#[derive(Clone)]
pub struct Publisher {
    engine: Arc<EngineHandle>,
    topic: String,
}

pub struct Subscription {
    rx: mpsc::Receiver<AckableMessage>,
}

pub struct AutoAckedSubscription {
    rx: mpsc::Receiver<Message>,
}

pub struct Message {
    pub offset: u64,
    pub payload: Vec<u8>,
}

impl Message {
    pub fn deserialize<T: DeserializeOwned>(&self) -> anyhow::Result<T> {
        Ok(rmp_serde::from_slice(&self.payload)?)
    }
}

pub struct AckableMessage {
    pub offset: u64,
    pub payload: Vec<u8>,
    ack: oneshot::Sender<u64>,
}

impl AckableMessage {
    // TODO: return simple message
    pub async fn ack(self) -> anyhow::Result<()> {
        // TODO: bubble errors
        let _ = self.ack.send(self.offset);
        Ok(())
    }

    pub fn deserialize<T: DeserializeOwned>(&self) -> anyhow::Result<T> {
        Ok(rmp_serde::from_slice(&self.payload)?)
    }
}

enum Waiter {
    Publish(oneshot::Sender<anyhow::Result<u64>>),
    SubscribeManual(oneshot::Sender<anyhow::Result<AckableSubChannel>>),
    SubscribeAuto(oneshot::Sender<anyhow::Result<AutoAckedSubChannel>>),
}

// ===== Client API =============================================================

impl Client {
    /// Connect to a server socket.
    pub async fn connect(addr: &str, opts: ClientOptions) -> anyhow::Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        let framed = Framed::new(stream, ProtoCodec);

        let engine = start_engine(framed, opts).await?;
        Ok(Client { engine })
    }

    /// Get a handle that you can use to publish messages to a specific topic.
    pub fn publisher(&self, topic: impl Into<String>) -> Publisher {
        Publisher {
            engine: self.engine.clone(),
            topic: topic.into(),
        }
    }

    /// Subscribe to receive messages from a topic, with manual acknowledgements.
    /// Messages must be acked explicitly; otherwise they may be redelivered.
    pub async fn subscribe(&self, req: Subscribe) -> anyhow::Result<Subscription> {
        let rx: Receiver<AckableMessage> = self.engine.subscribe(req).await?;
        Ok(Subscription { rx })
    }

    /// Subscribe to receive messages from a topic, with automatic acknowledgements.
    /// Messages that have been received by the client will not be redelivered.
    pub async fn subscribe_acked(&self, req: Subscribe) -> anyhow::Result<AutoAckedSubscription> {
        let rx: Receiver<Message> = self.engine.subscribe_auto_ack(req).await?;
        Ok(AutoAckedSubscription { rx })
    }
}

impl Publisher {
    pub async fn publish(&self, payload: Vec<u8>) -> anyhow::Result<()> {
        self.engine.publish(self.topic.clone(), payload).await
    }

    pub async fn publish_confirmed(&self, payload: Vec<u8>) -> anyhow::Result<u64> {
        self.engine
            .publish_confirmed(self.topic.clone(), payload)
            .await
    }
}

impl Subscription {
    pub async fn recv(&mut self) -> Option<AckableMessage> {
        self.rx.recv().await
    }

    pub fn into_stream(self) -> impl futures::Stream<Item = AckableMessage> {
        futures::stream::unfold(self, |mut s| async move {
            s.rx.recv().await.map(|msg| (msg, s))
        })
    }
}

impl AutoAckedSubscription {
    pub async fn recv(&mut self) -> Option<Message> {
        self.rx.recv().await
    }

    pub fn into_stream(self) -> impl futures::Stream<Item = Message> {
        futures::stream::unfold(self, |mut s| async move {
            s.rx.recv().await.map(|msg| (msg, s))
        })
    }
}

// ===== Engine =================================================================

#[derive(Clone)]
struct EngineHandle {
    tx: mpsc::Sender<Command>,
    shutdown: Arc<Notify>,
}

#[derive(Debug)]
enum Command {
    Publish {
        topic: String,
        payload: Vec<u8>,
    },
    PublishConfirmed {
        topic: String,
        payload: Vec<u8>,
        reply: oneshot::Sender<anyhow::Result<u64>>,
    },
    Subscribe {
        req: Subscribe,
        reply: oneshot::Sender<anyhow::Result<AckableSubChannel>>,
    },
    SubscribeAutoAcked {
        req: Subscribe,
        reply: oneshot::Sender<anyhow::Result<AutoAckedSubChannel>>,
    },
    Ack {
        sub_id: u64,
        delivery_tag_epoch: u64,
    },
}

#[derive(Debug)]
struct AutoAckedSubChannel {
    auto: mpsc::Receiver<Message>,
}

#[derive(Debug)]
struct AckableSubChannel {
    manual: mpsc::Receiver<AckableMessage>,
}

enum SubDelivery {
    Manual(mpsc::Sender<AckableMessage>),
    Auto(mpsc::Sender<Message>),
}

struct SubState {
    topic: String,
    group: String,
    partition: u32,
    delivery: SubDelivery,
}

impl Drop for Client {
    fn drop(&mut self) {
        self.engine.shutdown.notify_waiters();
    }
}

async fn start_engine(
    mut framed: Framed<TcpStream, ProtoCodec>,
    opts: ClientOptions,
) -> anyhow::Result<Arc<EngineHandle>> {
    let shutdown = Arc::new(Notify::new());
    // handshake
    framed
        .send(encode(
            Op::Hello,
            1,
            &Hello {
                client_name: opts.client_name.clone(),
                client_version: opts.client_version.clone(),
                protocol_version: PROTOCOL_V1,
            },
        ))
        .await?;

    let frame = framed
        .next()
        .await
        .ok_or_else(|| anyhow::anyhow!("EOF"))??;
    match frame.opcode {
        x if x == Op::HelloOk as u16 => {
            let ho: HelloOk = decode(&frame);
            if ho.compliance != COMPLIANCE_STRING {
                tracing::warn!(
                    id = "NF-SOVEREIGN-2025-GN-OPT-OUT-TDM",
                    expected = COMPLIANCE_STRING,
                    got = %ho.compliance,
                    "Invariant violated: compliance marker altered or missing"
                );
                anyhow::bail!("Protocol compliance marker mismatch");
            }
            if ho.protocol_version != PROTOCOL_V1 {
                anyhow::bail!("Protocol version mismatch");
            }
        }
        x if x == Op::HelloErr as u16 => {
            let e: ErrorMsg = decode(&frame);
            anyhow::bail!("HELLO failed: {}", e.message);
        }
        _ => anyhow::bail!("unexpected frame"),
    }

    if let Some(auth) = opts.auth {
        framed.send(encode(Op::Auth, 2, &auth)).await?;
        let frame = framed
            .next()
            .await
            .ok_or_else(|| anyhow::anyhow!("EOF"))??;
        match frame.opcode {
            x if x == Op::AuthOk as u16 => {}
            x if x == Op::AuthErr as u16 => {
                let e: ErrorMsg = decode(&frame);
                anyhow::bail!("AUTH failed: {}", e.message);
            }
            _ => anyhow::bail!("unexpected auth frame"),
        }
    }

    let (cmd_tx, mut cmd_rx) = mpsc::channel::<Command>(64);
    let handle = Arc::new(EngineHandle {
        tx: cmd_tx.clone(),
        shutdown: shutdown.clone(),
    });

    let subs = Arc::new(Mutex::new(HashMap::<u64, SubState>::new()));

    let shutdown_engine = shutdown.clone();
    let shutdown_acks = shutdown.clone();

    // writer + reader loop
    tokio::spawn(async move {
        let mut next_req = 1u64;
        let mut waiters: HashMap<u64, Waiter> = HashMap::new();

        loop {
            tokio::select! {
                Some(cmd) = cmd_rx.recv() => match cmd {
                    Command::Publish { topic, payload } => {
                        let req_id = next_req; next_req += 1;
                        let p = Publish {
                            topic,
                            partition: 0,
                            require_confirm: false,
                            payload,
                        };
                        let _ = framed.send(encode(Op::Publish, req_id, &p)).await;
                    }
                    Command::PublishConfirmed { topic, payload, reply } => {
                        let req_id = next_req; next_req += 1;
                        waiters.insert(req_id, Waiter::Publish(reply));
                        let p = Publish {
                            topic,
                            partition: 0,
                            require_confirm: true,
                            payload,
                        };
                        let _ = framed.send(encode(Op::Publish, req_id, &p)).await;
                    }
                    Command::Subscribe { req, reply } => {
                        let req_id = next_req; next_req += 1;
                        waiters.insert(req_id, Waiter::SubscribeManual(reply));
                        let _ = framed.send(encode(Op::Subscribe, req_id, &req)).await;
                    }
                    Command::SubscribeAutoAcked { req, reply } => {
                        let req_id = next_req; next_req += 1;
                        waiters.insert(req_id, Waiter::SubscribeAuto(reply));
                        let _ = framed.send(encode(Op::Subscribe, req_id, &req)).await;
                    }
                    Command::Ack { sub_id, delivery_tag_epoch } => {
                        if let Some(s) = subs.lock().await.get(&sub_id) {
                            let ack = Ack {
                                topic: s.topic.clone(),
                                group: s.group.clone(),
                                partition: s.partition,
                                tags: vec![DeliveryTag { epoch: delivery_tag_epoch}],
                            };
                            let _ = framed.send(encode(Op::Ack, 0, &ack)).await;
                        }
                    }
                },
                Some(frame) = framed.next() => {
                    let frame = match frame { Ok(f) => f, Err(_) => {
                        break;
                    } };
                    match frame.opcode {
                        x if x == Op::PublishOk as u16 => {
                            let ok: PublishOk = decode(&frame);

                            match waiters.remove(&frame.request_id) {
                                Some(Waiter::Publish(tx)) => {
                                    let _ = tx.send(Ok(ok.offset));
                                }
                                Some(_other) => {
                                    // protocol violation: PublishOk for non-publish request
                                    // log + drop
                                    // TODO
                                    tracing::error!("Internal error: Wrong request/response match")
                                }
                                None => {
                                    // unexpected PublishOk (fire-and-forget or stale)
                                    // log + drop
                                    // Server must not send PublishOk unless require_confirm = true.
                                    // TODO
                                    tracing::error!("Internal error: unexpected PublishOk")
                                }
                            }
                        }
                        x if x == Op::Deliver as u16 => {
                            let d: Deliver = decode(&frame);
                            if let Some(s) = subs.lock().await.get(&d.sub_id) {
                                match &s.delivery {
                                    SubDelivery::Manual(tx) => {
                                        let (ack_tx, ack_rx) = oneshot::channel();
                                        let msg = AckableMessage {
                                            offset: d.offset,
                                            payload: d.payload,
                                            ack: ack_tx,
                                        };

                                        if tx.send(msg).await.is_ok() {
                                            // TODO: will hang forever unless we cancel it? Investigate/fix
                                            tokio::select! {
                                                Ok(tag_epoch) = ack_rx => {
                                                    let _ = cmd_tx.send(Command::Ack { sub_id: d.sub_id, delivery_tag_epoch: tag_epoch  }).await;
                                                }
                                                _ = shutdown_acks.notified() => {
                                                    // engine is shutting down, drop silently
                                                }
                                            }
                                        }
                                    }

                                    SubDelivery::Auto(tx) => {
                                        let _ = tx.send(Message {
                                            offset: d.offset,
                                            payload: d.payload,
                                        }).await;
                                    }
                                }
                            }
                        }
                        x if x == Op::SubscribeOk as u16 => {
                            let ok: SubscribeOk = decode(&frame);

                            if let Some(waiter) = waiters.remove(&frame.request_id) {
                                match waiter {
                                    Waiter::SubscribeManual(tx) => {
                                        let (txm, rxm) = mpsc::channel(32);

                                        subs.lock().await.insert(ok.sub_id, SubState {
                                            topic: ok.topic.clone(),
                                            group: ok.group.clone(),
                                            partition: ok.partition,
                                            delivery: SubDelivery::Manual(txm),
                                        });

                                        let _ = tx.send(Ok(AckableSubChannel { manual: rxm }));
                                    }

                                    Waiter::SubscribeAuto(tx) => {
                                        let (txa, rxa) = mpsc::channel(32);

                                        subs.lock().await.insert(ok.sub_id, SubState {
                                            topic: ok.topic.clone(),
                                            group: ok.group.clone(),
                                            partition: ok.partition,
                                            delivery: SubDelivery::Auto(txa),
                                        });

                                        let _ = tx.send(Ok(AutoAckedSubChannel { auto: rxa }));
                                    }

                                    _ => {
                                        // protocol violation: SubscribeOk for non-subscribe request_id
                                        // TODO
                                        tracing::error!("Internal error: protocol violation: SubscribeOk for non-subscribe request_id")
                                    }
                                }
                            }
                        }
                        x if x == Op::Error as u16 => {
                            let err: ErrorMsg = decode(&frame);

                            if let Some(waiter) = waiters.remove(&frame.request_id) {
                                match waiter {
                                    Waiter::Publish(tx) => {
                                        let _ = tx.send(Err(anyhow::anyhow!(
                                            "publish error {}: {}",
                                            err.code,
                                            err.message
                                        )));
                                    }
                                    Waiter::SubscribeManual(tx) => {
                                        let _ = tx.send(Err(anyhow::anyhow!(
                                            "subscribe error {}: {}",
                                            err.code,
                                            err.message
                                        )));
                                    }
                                    Waiter::SubscribeAuto(tx) => {
                                        let _ = tx.send(Err(anyhow::anyhow!(
                                            "subscribe error {}: {}",
                                            err.code,
                                            err.message
                                        )));
                                    }
                                }
                            } else {
                                // connection-level error
                                // fail all waiters
                                let msg = format!("connection error {}: {}", err.code, err.message);
                                for (_, w) in waiters.drain() {
                                    fail_waiter(w, anyhow::anyhow!(msg.clone()));
                                }

                                // TODO: notify subscriptions
                                // TODO: possibly resubscribe
                                // TODO: possibly redeliver in-flight messages
                                // close all subscription channels
                                shutdown_engine.notify_waiters();

                                break; // or trigger reconnect
                            }
                        }
                        _ => {}
                    }
                }
            }
        }

        // ================================
        // FAIL ALL PENDING WAITERS
        // ================================

        for (_, waiter) in waiters.drain() {
            fail_waiter(waiter, anyhow::Error::msg("engine shutdown"));
        }

        // subs cleared
        subs.lock().await.clear();

        // notify shutdown listeners
        shutdown.notify_waiters();
    });

    Ok(handle)
}

fn fail_waiter(waiter: Waiter, err: anyhow::Error) {
    match waiter {
        Waiter::Publish(tx) => {
            let _ = tx.send(Err(err));
        }
        Waiter::SubscribeManual(tx) => {
            let _ = tx.send(Err(err));
        }
        Waiter::SubscribeAuto(tx) => {
            let _ = tx.send(Err(err));
        }
    }
}

impl EngineHandle {
    async fn publish(&self, topic: String, payload: Vec<u8>) -> anyhow::Result<()> {
        self.tx.send(Command::Publish { topic, payload }).await?;
        Ok(())
    }

    async fn publish_confirmed(&self, topic: String, payload: Vec<u8>) -> anyhow::Result<u64> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Command::PublishConfirmed {
                topic,
                payload,
                reply: tx,
            })
            .await?;
        rx.await?
    }

    async fn subscribe(&self, req: Subscribe) -> anyhow::Result<mpsc::Receiver<AckableMessage>> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(Command::Subscribe { req, reply: tx }).await?;
        let chans = rx.await??;
        Ok(chans.manual)
    }

    async fn subscribe_auto_ack(&self, req: Subscribe) -> anyhow::Result<mpsc::Receiver<Message>> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Command::SubscribeAutoAcked { req, reply: tx })
            .await?;
        let chans = rx.await??;
        Ok(chans.auto)
    }
}

// ===== Options ===============================================================

pub struct ClientOptions {
    pub client_name: String,
    pub client_version: String,
    pub auth: Option<Auth>,
}
