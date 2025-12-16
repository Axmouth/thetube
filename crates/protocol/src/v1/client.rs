use std::sync::atomic::{AtomicU64, Ordering};

use futures::{SinkExt, StreamExt};

use crate::v1::{Ack, Deliver, ErrorMsg, Hello, HelloOk, Op, PROTOCOL_V1, Publish, PublishOk, Subscribe, helper::{Conn, decode, encode}};
static REQ: AtomicU64 = AtomicU64::new(1);

fn next_req_id() -> u64 {
    REQ.fetch_add(1, Ordering::Relaxed)
}

pub async fn demo_client(mut conn: Conn) -> anyhow::Result<()> {
    // Hello
    let req = next_req_id();
    conn.send(encode(Op::Hello, req, &Hello {
        client_name: "demo".into(),
        client_version: "0.1".into(),
        protocol_version: PROTOCOL_V1,
    })).await?;

    // Wait for HelloOk/Err
    let frame = conn.next().await.ok_or_else(|| anyhow::anyhow!("closed"))??;
    match frame.opcode {
        x if x == Op::HelloOk as u16 => {
            let ok: HelloOk = decode(&frame);
            println!("negotiated v{}", ok.protocol_version);
        }
        x if x == Op::HelloErr as u16 => {
            return Err(anyhow::anyhow!("hello rejected"));
        }
        _ => return Err(anyhow::anyhow!("unexpected opcode {}", frame.opcode)),
    }

    // Subscribe
    let req = next_req_id();
    conn.send(encode(Op::Subscribe, req, &Subscribe {
        topic: "t1".into(),
        group: "g1".into(),
        prefetch: 100,
        auto_ack: true,
    })).await?;

    // Publish (require confirm)
    let req_pub = next_req_id();
    conn.send(encode(Op::Publish, req_pub, &Publish {
        topic: "t1".into(),
        partition: 0,
        require_confirm: true,
        payload: b"hello".to_vec(),
    })).await?;

    // Event loop
    while let Some(frame) = conn.next().await {
        let frame = frame?;
        match frame.opcode {
            x if x == Op::Deliver as u16 => {
                let d: Deliver = decode(&frame);
                println!("DELIVER off={} bytes={}", d.offset, d.payload.len());

                // Ack single (or batch later)
                conn.send(encode(Op::Ack, next_req_id(), &Ack {
                    topic: d.topic.clone(),
                    group: d.group.clone(),
                    partition: d.partition,
                    offsets: vec![d.delivery_tag],
                })).await?;
            }
            x if x == Op::PublishOk as u16 => {
                let ok: PublishOk = decode(&frame);
                println!("PUBLISH_OK offset={}", ok.offset);
            }
            x if x == Op::Error as u16 => {
                let e: ErrorMsg = decode(&frame);
                eprintln!("ERROR {}: {}", e.code, e.message);
            }
            _ => {}
        }
    }

    Ok(())
}
