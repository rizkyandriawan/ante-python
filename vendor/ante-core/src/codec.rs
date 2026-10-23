use crate::{Error, Result};

const FLAG_CONTENT_TYPE: u8 = 1 << 0;
const FLAG_MESSAGE_ID: u8 = 1 << 1;
const FLAG_CORRELATION_ID: u8 = 1 << 2;
const FLAG_REPLY_TO: u8 = 1 << 3;
const MAX_PROPERTY_LEN: usize = 4096;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MessageRecord {
    pub payload: Vec<u8>,
    pub content_type: Option<String>,
    pub message_id: Option<String>,
    pub correlation_id: Option<String>,
    pub reply_to: Option<String>,
}

impl MessageRecord {
    pub fn encode(&self) -> Vec<u8> {
        let mut flags = 0u8;
        if self.content_type.is_some() {
            flags |= FLAG_CONTENT_TYPE;
        }
        if self.message_id.is_some() {
            flags |= FLAG_MESSAGE_ID;
        }
        if self.correlation_id.is_some() {
            flags |= FLAG_CORRELATION_ID;
        }
        if self.reply_to.is_some() {
            flags |= FLAG_REPLY_TO;
        }

        let props = [
            self.content_type.as_deref(),
            self.message_id.as_deref(),
            self.correlation_id.as_deref(),
            self.reply_to.as_deref(),
        ];

        let mut out = Vec::with_capacity(self.payload.len() + 32);
        out.push(1);
        out.push(flags);
        out.extend_from_slice(&(self.payload.len() as u32).to_be_bytes());
        for value in props.into_iter().flatten() {
            {
                let bytes = value.as_bytes();
                out.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
                out.extend_from_slice(bytes);
            }
        }
        out.extend_from_slice(&self.payload);
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut cursor = Cursor::new(bytes);
        let version = cursor.u8()?;
        if version != 1 {
            return Err(Error::Invalid(format!(
                "unsupported message version {version}"
            )));
        }
        let flags = cursor.u8()?;
        let payload_len = cursor.u32()? as usize;
        let mut read_prop = |bit: u8| -> Result<Option<String>> {
            if flags & bit == 0 {
                return Ok(None);
            }
            let len = cursor.u16()? as usize;
            if len > MAX_PROPERTY_LEN {
                return Err(Error::Invalid("property too large".into()));
            }
            let bytes = cursor.take(len)?;
            String::from_utf8(bytes.to_vec())
                .map(Some)
                .map_err(|_| Error::Invalid("property is not utf-8".into()))
        };
        let content_type = read_prop(FLAG_CONTENT_TYPE)?;
        let message_id = read_prop(FLAG_MESSAGE_ID)?;
        let correlation_id = read_prop(FLAG_CORRELATION_ID)?;
        let reply_to = read_prop(FLAG_REPLY_TO)?;
        let payload = cursor.take(payload_len)?.to_vec();
        if !cursor.is_empty() {
            return Err(Error::Invalid("trailing bytes in message record".into()));
        }
        Ok(MessageRecord {
            payload,
            content_type,
            message_id,
            correlation_id,
            reply_to,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeliveryRecord {
    pub delivery_id: u64,
    pub queue_id: u64,
    pub deadline_ms: u64,
}

impl DeliveryRecord {
    pub fn encode(self) -> [u8; 24] {
        let mut out = [0u8; 24];
        out[0..8].copy_from_slice(&self.delivery_id.to_be_bytes());
        out[8..16].copy_from_slice(&self.queue_id.to_be_bytes());
        out[16..24].copy_from_slice(&self.deadline_ms.to_be_bytes());
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != 24 {
            return Err(Error::Invalid("corrupt delivery record".into()));
        }
        let mut a = [0u8; 8];
        a.copy_from_slice(&bytes[0..8]);
        let delivery_id = u64::from_be_bytes(a);
        a.copy_from_slice(&bytes[8..16]);
        let queue_id = u64::from_be_bytes(a);
        a.copy_from_slice(&bytes[16..24]);
        let deadline_ms = u64::from_be_bytes(a);
        Ok(DeliveryRecord {
            delivery_id,
            queue_id,
            deadline_ms,
        })
    }
}

struct Cursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Cursor { bytes, pos: 0 }
    }
    fn take(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self
            .pos
            .checked_add(len)
            .ok_or_else(|| Error::Invalid("overflow".into()))?;
        if end > self.bytes.len() {
            return Err(Error::Invalid("truncated record".into()));
        }
        let slice = &self.bytes[self.pos..end];
        self.pos = end;
        Ok(slice)
    }
    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16> {
        let b = self.take(2)?;
        Ok(u16::from_be_bytes([b[0], b[1]]))
    }
    fn u32(&mut self) -> Result<u32> {
        let b = self.take(4)?;
        Ok(u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }
    fn is_empty(&self) -> bool {
        self.pos == self.bytes.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_roundtrip_with_properties_and_binary_payload() {
        let record = MessageRecord {
            payload: vec![0, 1, 2, 255, 10, 13],
            content_type: Some("application/json".into()),
            message_id: Some("m-1".into()),
            correlation_id: None,
            reply_to: Some("reply".into()),
        };
        let encoded = record.encode();
        assert_eq!(MessageRecord::decode(&encoded).unwrap(), record);
    }

    #[test]
    fn rejects_truncated_record() {
        let record = MessageRecord {
            payload: b"hello".to_vec(),
            ..Default::default()
        };
        let encoded = record.encode();
        assert!(MessageRecord::decode(&encoded[..encoded.len() - 1]).is_err());
    }
}
