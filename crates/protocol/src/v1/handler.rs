use std::{collections::HashMap, sync::Arc};

use anyhow::Context;
use futures::{SinkExt, StreamExt};
use tokio::{net::TcpListener, sync::mpsc};
use tokio_util::codec::Framed;
use fibril_broker::{AckRequest, Broker, ConsumerConfig, ConsumerHandle, coordination::Coordination};
use 
    crate::v1::{
        frame::{Frame, ProtoCodec},
        helper::{decode, encode},
        *,
    };
use fibril_storage::{Group, Partition, Topic};

type SubKey = (Topic, Group, Partition); // (topic, group, partition)

struct ConnState {
    authenticated: bool,
    subs: HashMap<SubKey, SubState>,
}

struct SubscriptionHandle {
    topic: String,
    group: String,
    consumer: ConsumerHandle,
}

struct SubState {
    auto_ack: bool,
    acker: tokio::sync::mpsc::Sender<AckRequest>,
}

pub async fn run_server<C: Coordination + Send + Sync + 'static>(
    addr: &str,
    broker: Arc<Broker<C>>,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    println!("listening on {}", addr);

    loop {
        let (socket, peer) = listener.accept().await?;
        let broker = broker.clone();

        tokio::spawn(async move {
            if let Err(e) = handle_connection(socket, broker).await {
                eprintln!("conn {} error: {:?}", peer, e);
            }
        });
    }
}

pub async fn handle_connection<C: Coordination + Send + Sync + 'static>(
    socket: tokio::net::TcpStream,
    broker: Arc<Broker<C>>,
) -> anyhow::Result<()> {
    // ---- Framed socket -----------------------------------------------------
    let framed = Framed::new(socket, ProtoCodec);
    let (mut writer, mut reader) = framed.split();

    // ---- Write fan-in channel ---------------------------------------------
    let (tx, mut rx) = mpsc::channel::<Frame>(256);

    // ---- Writer task -------------------------------------------------------
    let writer_task = tokio::spawn(async move {
        while let Some(frame) = rx.recv().await {
            if writer.send(frame).await.is_err() {
                break;
            }
        }
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
    while let Some(frame) = reader.next().await {
        let frame = frame?;

        match frame.opcode {
            // -------- AUTH ---------------------------------------------------
            x if x == Op::Auth as u16 => {
                let auth: Auth = decode(&frame);

                // TODO: real auth backend
                if auth.username == "guest" && auth.password == "guest" {
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
                }
            }

            // -------- SUBSCRIBE ---------------------------------------------
            x if x == Op::Subscribe as u16 => {
                let sub: Subscribe = decode(&frame);

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
                    messages, acker, ..
                } = consumer;

                let auto_ack = sub.auto_ack;
                let tx_clone = tx.clone();

                let key: SubKey = (sub.topic.clone(), sub.group.clone(), consumer.partition);

                let acker_clone = acker.clone();
                tokio::spawn(async move {
                    let mut rx = messages;

                    while let Some(msg) = rx.recv().await {
                        let deliver = Deliver {
                            topic: msg.message.topic.clone(),
                            group: msg.group.clone(),
                            partition: msg.message.partition,
                            offset: msg.message.offset,
                            delivery_tag: msg.delivery_tag,
                            payload: msg.message.payload.clone(),
                        };

                        // 1. Try to write to socket
                        if tx_clone
                            .send(encode(Op::Deliver, 0, &deliver))
                            .await
                            .is_err()
                        {
                            // Socket dead → do NOT auto-ack
                            break;
                        }

                        // 2. Auto-ack ONLY after successful send
                        if auto_ack {
                            let _ = acker.send(AckRequest { delivery_tag: msg.delivery_tag }).await;
                        }
                    }
                });

                state.subs.insert(
                    key,
                    SubState {
                        auto_ack: sub.auto_ack,
                        acker: acker_clone,
                    },
                );

                tx.send(encode(
                    Op::SubscribeOk,
                    frame.request_id,
                    &SubscribeOk {
                        topic: sub.topic,
                        group: sub.group,
                        partition: 0,
                    },
                ))
                .await?;
            }

            // -------- ACK ----------------------------------------------------
            x if x == Op::Ack as u16 => {
                // TODO: Decline ack when auto ack? Log?
                let ack: Ack = decode(&frame);

                let key: SubKey = (ack.topic.clone(), ack.group.clone(), ack.partition);

                if let Some(sub) = state.subs.get(&key)
                    && !sub.auto_ack {
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

    Ok(())
}
