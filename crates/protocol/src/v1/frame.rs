use bytes::{BytesMut, Buf, BufMut};
use tokio_util::codec::{Decoder, Encoder};
use std::io;

#[derive(Debug)]
pub struct Frame {
    pub version: u16,
    pub opcode: u16,
    pub flags: u32,
    pub request_id: u64,
    pub payload: Vec<u8>,
}

pub struct ProtoCodec;

impl Decoder for ProtoCodec {
    type Item = Frame;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Frame>, io::Error> {
        const HEADER: usize = 4 + 2 + 2 + 4 + 8; // 20
        if src.len() < HEADER {
            return Ok(None);
        }

        // Peek length (payload only)
        let mut peek = &src[..];
        let payload_len = peek.get_u32() as usize;

        if src.len() < HEADER + payload_len {
            return Ok(None);
        }

        // Now consume
        let payload_len = src.get_u32() as usize;
        let version = src.get_u16();
        let opcode = src.get_u16();
        let flags = src.get_u32();
        let request_id = src.get_u64();

        let payload = src.split_to(payload_len).to_vec();

        Ok(Some(Frame { version, opcode, flags, request_id, payload }))
    }
}

impl Encoder<Frame> for ProtoCodec {
    type Error = io::Error;

    fn encode(&mut self, item: Frame, dst: &mut BytesMut) -> Result<(), io::Error> {
        let payload_len = item.payload.len();
        dst.reserve(20 + payload_len);

        dst.put_u32(payload_len as u32);
        dst.put_u16(item.version);
        dst.put_u16(item.opcode);
        dst.put_u32(item.flags);
        dst.put_u64(item.request_id);
        dst.extend_from_slice(&item.payload);

        Ok(())
    }
}
