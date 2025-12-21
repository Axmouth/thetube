use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use crate::v1::{
    frame::{Frame, ProtoCodec},
    helper::{decode, encode},
    *,
};
use anyhow::Context;
use fibril_broker::{
    AckRequest, Broker, ConsumerConfig, ConsumerHandle, coordination::Coordination,
};
use fibril_storage::{Group, LogId, Topic};
use futures::{SinkExt, StreamExt};
use tokio::{net::TcpListener, sync::mpsc};
use tokio_util::codec::Framed;

type SubKey = (Topic, Group); // (topic, group)

struct ConnState {
    authenticated: bool,
    subs: HashMap<SubKey, SubState>,
}

struct SubState {
    sub_id: u64,
    auto_ack: bool,
    acker: tokio::sync::mpsc::Sender<AckRequest>,
    task: tokio::task::JoinHandle<()>,
}

pub async fn run_server<C: Coordination + Send + Sync + 'static>(
    addr: SocketAddr,
    broker: Arc<Broker<C>>,
    auth: Option<impl AuthHandler + Send + Sync + Clone + 'static>,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    tracing::info!("listening on {}", addr);

    loop {
        let (socket, peer) = listener.accept().await?;
        let broker = broker.clone();

        let auth = auth.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_connection(socket, broker, auth).await {
                tracing::error!("conn {} error: {:?}", peer, e);
            }
        });
    }
}

pub async fn handle_connection<C: Coordination + Send + Sync + 'static>(
    socket: tokio::net::TcpStream,
    broker: Arc<Broker<C>>,
    auth_handler: Option<impl AuthHandler + Send + Sync>,
) -> anyhow::Result<()> {
    // ---- Framed socket -----------------------------------------------------
    let peer_addr = socket.peer_addr().ok();
    let framed = Framed::new(socket, ProtoCodec);
    let (mut writer, mut reader) = framed.split();

    // ---- Write fan-in channel ---------------------------------------------
    let (tx, mut rx) = mpsc::channel::<Frame>(256);

    // ---- Writer task -------------------------------------------------------
    let writer_task = tokio::spawn(async move {
        tracing::debug!("[writer] START");

        while let Some(frame) = rx.recv().await {
            tracing::debug!(
                "[writer] Writing Frame to tcp socket.. code={}",
                frame.opcode
            );
            if let Err(err) = writer.send(frame).await {
                tracing::error!("[writer] Error writing to tcp socket : {err}");
                break;
            }
        }

        tracing::debug!("[writer] EXIT");
    });

    // ---- Connection state --------------------------------------------------
    let mut state = ConnState {
        authenticated: false,
        subs: HashMap::new(),
    };

    // ---- HELLO handshake ---------------------------------------------------
    let frame = reader
        .next()
        .await
        .context("connection closed before HELLO")??;

    if frame.opcode != Op::Hello as u16 {
        tx.send(encode(
            Op::Error,
            frame.request_id,
            &ErrorMsg {
                code: 400,
                message: "expected HELLO".into(),
            },
        ))
        .await
        .ok();
        return Ok(());
    }

    let hello: Hello = decode(&frame);

    if hello.protocol_version != PROTOCOL_V1 {
        tx.send(encode(
            Op::HelloErr,
            frame.request_id,
            &ErrorMsg {
                code: 1,
                message: "unsupported protocol version".into(),
            },
        ))
        .await
        .ok();
        return Ok(());
    }

    tx.send(encode(
        Op::HelloOk,
        frame.request_id,
        &HelloOk {
            protocol_version: PROTOCOL_V1,
            server_name: "rust-broker".into(),
        },
    ))
    .await?;

    // ---- Main reader loop --------------------------------------------------
    while let Some(frame) = tokio::time::timeout(std::time::Duration::from_secs(30), reader.next()).await? {
        let frame = frame?;

        let auth_required = auth_handler.is_some();
        let is_auth = frame.opcode == Op::Auth as u16;
        let is_ping = frame.opcode == Op::Ping as u16;

        if auth_required && !state.authenticated && !(is_auth || is_ping) {
            tx.send(encode(
                Op::Error,
                frame.request_id,
                &ErrorMsg {
                    code: 401,
                    message: "authentication required".into(),
                },
            ))
            .await?;

            break; // close connection
        }

        match frame.opcode {
            // -------- AUTH ---------------------------------------------------
            x if x == Op::Auth as u16 => {
                if state.authenticated {
                    tx.send(encode(
                        Op::AuthErr,
                        frame.request_id,
                        &ErrorMsg {
                            code: 401,
                            message: "already authenticated".into(),
                        },
                    ))
                    .await?;
                } else if auth_handler.is_none() {
                    tx.send(encode(
                        Op::AuthErr,
                        frame.request_id,
                        &ErrorMsg {
                            code: 400,
                            message: "authentication not applicable".into(),
                        },
                    ))
                    .await?;
                } else {
                    // ---- AUTH handshake -------------------------------------
                    if let Some(auth_handler) = &auth_handler {
                        let auth_frame: Auth = decode(&frame);

                        let verified = auth_handler
                            .verify(&auth_frame.username, &auth_frame.password)
                            .await;

                        if verified {
                            state.authenticated = true;
                            tx.send(encode(Op::AuthOk, frame.request_id, &())).await?;
                        } else {
                            tx.send(encode(
                                Op::AuthErr,
                                frame.request_id,
                                &ErrorMsg {
                                    code: 401,
                                    message: "invalid credentials".into(),
                                },
                            ))
                            .await?;

                            break; // close connection
                        }
                    }
                }
            }

            // -------- SUBSCRIBE ---------------------------------------------
            x if x == Op::Subscribe as u16 => {
                let sub: Subscribe = decode(&frame);

                let sub_key: SubKey = (sub.topic.clone(), sub.group.clone());

                if state.subs.contains_key(&sub_key) {
                    tx.send(encode(
                        Op::SubscribeErr,
                        frame.request_id,
                        &ErrorMsg {
                            code: 409,
                            message: "already subscribed".into(),
                        },
                    ))
                    .await?;
                    continue;
                }

                let consumer = broker
                    .subscribe(
                        &sub.topic,
                        &sub.group,
                        ConsumerConfig {
                            prefetch_count: sub.prefetch as usize,
                        },
                    )
                    .await
                    .context("subscribe failed")?;

                let ConsumerHandle {
                    messages,
                    acker,
                    sub_id,
                    ..
                } = consumer;

                let auto_ack = sub.auto_ack;
                let tx_clone = tx.clone();

                let acker_clone = acker.clone();
                let handle = tokio::spawn(async move {
                    let mut rx = messages;

                    while let Some(msg) = rx.recv().await {
                        let deliver = Deliver {
                            sub_id,
                            topic: msg.message.topic.clone(),
                            group: msg.group.clone(),
                            partition: msg.message.partition,
                            offset: msg.message.offset,
                            delivery_tag: msg.delivery_tag,
                            payload: msg.message.payload.clone(),
                        };

                        tracing::debug!("Sending Deliver");

                        // 1. Try to write to socket
                        if let Err(err) = tx_clone.send(encode(Op::Deliver, 0, &deliver)).await {
                            // Socket dead -> do NOT auto-ack
                            tracing::error!("Failed to send to socket writer : {err}");
                            break;
                        }

                        // 2. Auto-ack ONLY after successful send
                        if auto_ack {
                            let _ = acker
                                .send(AckRequest {
                                    delivery_tag: msg.delivery_tag,
                                })
                                .await;
                        }
                    }
                });

                state.subs.insert(
                    sub_key,
                    SubState {
                        sub_id,
                        task: handle,
                        auto_ack: sub.auto_ack,
                        acker: acker_clone,
                    },
                );

                tx.send(encode(
                    Op::SubscribeOk,
                    frame.request_id,
                    &SubscribeOk {
                        sub_id,
                        topic: sub.topic,
                        group: sub.group,
                        partition: consumer.partition,
                    },
                ))
                .await?;
            }

            // -------- ACK ----------------------------------------------------
            x if x == Op::Ack as u16 => {
                // TODO: Decline ack when auto ack? Log?
                let ack: Ack = decode(&frame);

                let key: SubKey = (ack.topic.clone(), ack.group.clone());

                if let Some(sub) = state.subs.get(&key)
                    && !sub.auto_ack
                {
                    for off in ack.offsets {
                        let req = AckRequest { delivery_tag: off };
                        let _ = sub.acker.send(req).await;
                    }
                }
                // Unknown subscription: ignore (idempotent)
            }

            // -------- PUBLISH ------------------------------------------------
            x if x == Op::Publish as u16 => {
                let pubreq: Publish = decode(&frame);

                // TODO: USE PUBLISHER(cache them in a dashmap?)
                match broker.publish(&pubreq.topic, &pubreq.payload).await {
                    Ok(offset) => {
                        if pubreq.require_confirm {
                            tx.send(encode(
                                Op::PublishOk,
                                frame.request_id,
                                &PublishOk { offset },
                            ))
                            .await?;
                        }
                    }
                    Err(err) => {
                        tx.send(encode(
                            Op::Error,
                            frame.request_id,
                            &ErrorMsg {
                                code: 500,
                                message: err.to_string(),
                            },
                        ))
                        .await?;
                    }
                }
            }

            // -------- PING ---------------------------------------------------
            x if x == Op::Ping as u16 => {
                tx.send(encode(Op::Pong, frame.request_id, &())).await?;
            }

            // -------- UNKNOWN -----------------------------------------------
            _ => {
                tx.send(encode(
                    Op::Error,
                    frame.request_id,
                    &ErrorMsg {
                        code: 400,
                        message: "unknown opcode".into(),
                    },
                ))
                .await?;
            }
        }
    }

    // ---- Connection closing ------------------------------------------------
    drop(tx); // closes writer channel
    let _ = writer_task.await;

    for (_, sub) in state.subs.drain() {
        sub.task.abort();
    }

    tracing::debug!("[conn] EXIT handle_connection peer={:?}", peer_addr);

    Ok(())
}
