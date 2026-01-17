use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Instant};

use crate::v1::{
    frame::{Frame, ProtoCodec},
    helper::{decode, encode},
    *,
};
use anyhow::Context;
use fibril_broker::{
    Broker, ConsumerConfig, ConsumerHandle, SettleRequest, SettleType, coordination::Coordination,
};
use fibril_metrics::{ConnectionStats, TcpStats};
use fibril_storage::{AppendReceiptExt, Group, Offset, Topic};
use fibril_util::AuthHandler;
use futures::{SinkExt, StreamExt};
use tokio::{net::TcpListener, sync::mpsc};
use tokio_util::codec::Framed;
use uuid::Uuid;

type SubKey = (Topic, Group); // (topic, group)

struct ConnState {
    authenticated: bool,
    subs: HashMap<SubKey, SubState>,
}

struct SubState {
    sub_id: u64,
    auto_ack: bool,
    acker: tokio::sync::mpsc::Sender<SettleRequest>,
    task: tokio::task::JoinHandle<()>,
}

pub async fn run_server<C: Coordination + Send + Sync + 'static>(
    addr: SocketAddr,
    broker: Arc<Broker<C>>,
    tcp_stats: Arc<TcpStats>,
    connection_stats: Arc<ConnectionStats>,
    auth: Option<impl AuthHandler + Send + Sync + Clone + 'static>,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    print_banner(&addr);

    loop {
        let (socket, peer) = listener.accept().await?;
        tcp_stats.connection_opened();
        let conn_id = connection_stats.add_connection(peer, Instant::now(), false);
        let broker = broker.clone();

        let auth = auth.clone();
        let tcp_stats = tcp_stats.clone();
        let connection_stats = connection_stats.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_connection(
                socket,
                broker,
                tcp_stats.clone(),
                connection_stats.clone(),
                conn_id,
                auth,
            )
            .await
            {
                tracing::error!("conn {} error: {:?}", peer, e);
            }

            connection_stats.remove_connection(&conn_id);
        });
    }
}

pub async fn handle_connection<C: Coordination + Send + Sync + 'static>(
    socket: tokio::net::TcpStream,
    broker: Arc<Broker<C>>,
    tcp_stats: Arc<TcpStats>,
    connection_stats: Arc<ConnectionStats>,
    conn_id: Uuid,
    auth_handler: Option<impl AuthHandler + Send + Sync>,
) -> anyhow::Result<()> {
    // ---- Framed socket -----------------------------------------------------
    let peer_addr = socket.peer_addr().ok();
    let framed = Framed::new(socket, ProtoCodec);
    let (mut writer, mut reader) = framed.split();

    // ---- Write fan-in channel ---------------------------------------------
    let (tx, mut rx) = mpsc::channel::<Frame>(256);

    let metrics_clone = tcp_stats.clone();
    // ---- Writer task -------------------------------------------------------
    let writer_task = tokio::spawn(async move {
        tracing::debug!("[writer] START");

        let metrics = metrics_clone.clone();
        while let Some(frame) = rx.recv().await {
            tracing::debug!(
                "[writer] Writing Frame to tcp socket.. code={}",
                frame.opcode
            );
            let size = size_of_val(&frame) + frame.payload.len();
            if let Err(err) = writer.send(frame).await {
                metrics.error();
                tracing::error!("[writer] Error writing to tcp socket : {err}");
                break;
            } else {
                metrics.bytes_out(size as u64);
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
        tcp_stats.error();
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
        tcp_stats.error();
        return Ok(());
    }

    let hello_ok = &HelloOk {
        protocol_version: PROTOCOL_V1,
        server_name: "rust-broker".into(),
        compliance: "v=1;license=MIT;ai_train=disallowed;policy=AI_POLICY.md".to_owned(),
    };

    // TODO: Two ways handshake?
    if hello_ok.compliance != COMPLIANCE_STRING {
        tracing::warn!(
            id = "NF-SOVEREIGN-2025-GN-OPT-OUT-TDM",
            expected = COMPLIANCE_STRING,
            got = %hello_ok.compliance,
            "Invariant violated: compliance marker altered or missing"
        );
        anyhow::bail!("Protocol compliance marker mismatch");
    }

    tx.send(encode(Op::HelloOk, frame.request_id, &hello_ok))
        .await?;

    // ---- Main reader loop --------------------------------------------------
    while let Some(frame) =
        tokio::time::timeout(std::time::Duration::from_secs(30), reader.next()).await?
    {
        let metrics = tcp_stats.clone();
        let frame = frame?;
        let size = size_of_val(&frame) + frame.payload.len();
        metrics.bytes_in(size as u64);

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
            metrics.error();

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
                    metrics.error();
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
                    metrics.error();
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
                            connection_stats.set_connection_auth(&conn_id, true);
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
                            metrics.error();

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
                    metrics.error();
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
                    settler: acker,
                    sub_id,
                    ..
                } = consumer;

                let auto_ack = sub.auto_ack;
                let tx_clone = tx.clone();

                connection_stats.add_sub(
                    &conn_id,
                    sub.topic.clone(),
                    sub.group.clone(),
                    Instant::now(),
                    auto_ack,
                );

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
                            metrics.error();
                            break;
                        }

                        // 2. Auto-ack ONLY after successful send
                        if auto_ack {
                            let _ = acker
                                .send(SettleRequest {
                                    settle_type: SettleType::Ack,
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
                    for tag in ack.tags {
                        let req = SettleRequest {
                            delivery_tag: tag,
                            settle_type: SettleType::Ack,
                        };
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
                        metrics.error();
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
                metrics.error();
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

    tcp_stats.connection_closed();
    Ok(())
}

pub fn print_banner(bind: &SocketAddr) {
    let art = ASCII_ARTS[0];

    tracing::info!("\n{art}\nListening on {bind}\n");
}

const ASCII_ARTS: &[&str] = &[
    r#"
███████╗██╗██████╗ ██████╗ ██╗██╗     
██╔════╝██║██╔══██╗██╔══██╗██║██║     
█████╗  ██║██████╔╝██████╔╝██║██║     
██╔══╝  ██║██╔══██╗██╔══██╗██║██║     
██║     ██║██████╔╝██║  ██║██║███████╗
╚═╝     ╚═╝╚═════╝ ╚═╝  ╚═╝╚═╝╚══════╝
                                         
                                         
"#,
    r#"
                                                  
88888888888  88  88                       88  88  
88           ""  88                       ""  88  
88               88                           88  
88aaaaa      88  88,dPPYba,   8b,dPPYba,  88  88  
88"""""      88  88P'    "8a  88P'   "Y8  88  88  
88           88  88       d8  88          88  88  
88           88  88b,   ,a8"  88          88  88  
88           88  8Y"Ybbd8"'   88          88  88  
                                                  
                                                  
"#,
    r#"
'||''''|  ||  '||               ||  '||  
 ||  .   ...   || ...  ... ..  ...   ||  
 ||''|    ||   ||'  ||  ||' ''  ||   ||  
 ||       ||   ||    |  ||      ||   ||  
.||.     .||.  '|...'  .||.    .||. .||. 
                                         
                                         
"#,
    r#"
'||''''|      '||                 '||` 
 ||  .    ''   ||             ''   ||  
 ||''|    ||   ||''|, '||''|  ||   ||  
 ||       ||   ||  ||  ||     ||   ||  
.||.     .||. .||..|' .||.   .||. .||. 
                                       
                                       
"#,
    r#"
 ▄▄▄▄▄▄▄▄▄▄▄  ▄▄▄▄▄▄▄▄▄▄▄  ▄▄▄▄▄▄▄▄▄▄   ▄▄▄▄▄▄▄▄▄▄▄  ▄▄▄▄▄▄▄▄▄▄▄  ▄           
▐░░░░░░░░░░░▌▐░░░░░░░░░░░▌▐░░░░░░░░░░▌ ▐░░░░░░░░░░░▌▐░░░░░░░░░░░▌▐░▌          
▐░█▀▀▀▀▀▀▀▀▀  ▀▀▀▀█░█▀▀▀▀ ▐░█▀▀▀▀▀▀▀█░▌▐░█▀▀▀▀▀▀▀█░▌ ▀▀▀▀█░█▀▀▀▀ ▐░▌          
▐░▌               ▐░▌     ▐░▌       ▐░▌▐░▌       ▐░▌     ▐░▌     ▐░▌          
▐░█▄▄▄▄▄▄▄▄▄      ▐░▌     ▐░█▄▄▄▄▄▄▄█░▌▐░█▄▄▄▄▄▄▄█░▌     ▐░▌     ▐░▌          
▐░░░░░░░░░░░▌     ▐░▌     ▐░░░░░░░░░░▌ ▐░░░░░░░░░░░▌     ▐░▌     ▐░▌          
▐░█▀▀▀▀▀▀▀▀▀      ▐░▌     ▐░█▀▀▀▀▀▀▀█░▌▐░█▀▀▀▀█░█▀▀      ▐░▌     ▐░▌          
▐░▌               ▐░▌     ▐░▌       ▐░▌▐░▌     ▐░▌       ▐░▌     ▐░▌          
▐░▌           ▄▄▄▄█░█▄▄▄▄ ▐░█▄▄▄▄▄▄▄█░▌▐░▌      ▐░▌  ▄▄▄▄█░█▄▄▄▄ ▐░█▄▄▄▄▄▄▄▄▄ 
▐░▌          ▐░░░░░░░░░░░▌▐░░░░░░░░░░▌ ▐░▌       ▐░▌▐░░░░░░░░░░░▌▐░░░░░░░░░░░▌
 ▀            ▀▀▀▀▀▀▀▀▀▀▀  ▀▀▀▀▀▀▀▀▀▀   ▀         ▀  ▀▀▀▀▀▀▀▀▀▀▀  ▀▀▀▀▀▀▀▀▀▀▀ 
                                                                              
"#,
    r#"
                                                                                    
                             bbbbbbbb                                               
FFFFFFFFFFFFFFFFFFFFFF  iiii b::::::b                                 iiii  lllllll 
F::::::::::::::::::::F i::::ib::::::b                                i::::i l:::::l 
F::::::::::::::::::::F  iiii b::::::b                                 iiii  l:::::l 
FF::::::FFFFFFFFF::::F        b:::::b                                       l:::::l 
  F:::::F       FFFFFFiiiiiii b:::::bbbbbbbbb    rrrrr   rrrrrrrrr  iiiiiii  l::::l 
  F:::::F             i:::::i b::::::::::::::bb  r::::rrr:::::::::r i:::::i  l::::l 
  F::::::FFFFFFFFFF    i::::i b::::::::::::::::b r:::::::::::::::::r i::::i  l::::l 
  F:::::::::::::::F    i::::i b:::::bbbbb:::::::brr::::::rrrrr::::::ri::::i  l::::l 
  F:::::::::::::::F    i::::i b:::::b    b::::::b r:::::r     r:::::ri::::i  l::::l 
  F::::::FFFFFFFFFF    i::::i b:::::b     b:::::b r:::::r     rrrrrrri::::i  l::::l 
  F:::::F              i::::i b:::::b     b:::::b r:::::r            i::::i  l::::l 
  F:::::F              i::::i b:::::b     b:::::b r:::::r            i::::i  l::::l 
FF:::::::FF           i::::::ib:::::bbbbbb::::::b r:::::r           i::::::il::::::l
F::::::::FF           i::::::ib::::::::::::::::b  r:::::r           i::::::il::::::l
F::::::::FF           i::::::ib:::::::::::::::b   r:::::r           i::::::il::::::l
FFFFFFFFFFF           iiiiiiiibbbbbbbbbbbbbbbb    rrrrrrr           iiiiiiiillllllll
                                                                                    
                                                                                    
                                                                                    
                                                                                    
                                                                                    
                                                                                    
                                                                                    
"#,
];
