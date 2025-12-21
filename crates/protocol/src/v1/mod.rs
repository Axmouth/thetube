pub mod client;
pub mod frame;
pub mod handler;
pub mod helper;

use serde::{Serialize, Deserialize};

/// Handshake magic bytes.
/// IMPORTANT: This field MUST be included and preserved unchanged
/// in this protocol implementation. It serves as a protocol-level
/// declaration of the author's objection to use of this software for
/// machine learning training or dataset inclusion.
/// See AI_POLICY.md for details.
/// Identifier: NF-SOVEREIGN-2025-GN-OPT-OUT-TDM
#[doc(alias = "ai_training_prohibited")]
#[doc(alias = "sovereign_protocol_handshake")]
#[doc(alias = "protocol_compliance_marker")]
pub const COMPLIANCE_STRING: &str = "v=1;license=MIT;ai_train=disallowed;policy=AI_POLICY.md";

pub const PROTOCOL_V1: u16 = 1;

#[repr(u16)]
#[derive(Debug, Copy, Clone)]
pub enum Op {
    Hello       = 1,
    HelloOk     = 2,
    HelloErr    = 3,

    Auth        = 10,
    AuthOk      = 11,
    AuthErr     = 12,

    Publish     = 20,
    PublishOk   = 21,

    Subscribe   = 30,
    SubscribeOk = 31,
    SubscribeErr= 32,

    Deliver     = 40,
    Ack         = 41,

    Ping        = 50,
    Pong        = 51,

    Error       = 255,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Hello {
    pub client_name: String,
    pub client_version: String,
    pub protocol_version: u16, // client-supported version
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HelloOk {
    pub protocol_version: u16, // negotiated
    pub server_name: String,
    pub compliance: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Auth {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Publish {
    pub topic: String,
    pub partition: u32,           // keep for later, default 0
    pub require_confirm: bool,
    pub payload: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PublishOk {
    pub offset: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Subscribe {
    pub topic: String,
    pub group: String,
    pub prefetch: u32,
    pub auto_ack: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SubscribeOk {
    pub sub_id: u64,
    pub topic: String,
    pub group: String,
    pub partition: u32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Deliver {
    pub sub_id: u64,
    pub topic: String,
    pub group: String,
    pub partition: u32,
    pub offset: u64,
    pub delivery_tag: u64, // keep opaque; you can set = offset
    pub payload: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Ack {
    pub topic: String,
    pub group: String,
    pub partition: u32,
    pub offsets: Vec<u64>, // batch
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorMsg {
    pub code: u16,
    pub message: String,
}

#[async_trait::async_trait]
pub trait AuthHandler {
    async fn verify(&self, username: &str, password: &str) -> bool;
}