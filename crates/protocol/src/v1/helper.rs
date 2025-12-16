use serde::{Deserialize, Serialize};
use tokio_util::codec::Framed;
use tokio::net::TcpStream;

use crate::v1::{Op, PROTOCOL_V1, frame::{Frame, ProtoCodec}};

pub type Conn = Framed<TcpStream, ProtoCodec>;

// TODO: Error handling :x
pub fn encode<T: Serialize>(op: Op, req_id: u64, msg: &T) -> Frame {
    let payload = rmp_serde::to_vec_named(msg).expect("serialize");
    Frame {
        version: PROTOCOL_V1,
        opcode: op as u16,
        flags: 0,
        request_id: req_id,
        payload,
    }
}

pub fn decode<T: for<'de> Deserialize<'de>>(frame: &Frame) -> T {
    rmp_serde::from_slice(&frame.payload).expect("deserialize")
}
