use std::io;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const FRAME_METHOD: u8 = 1;
pub const FRAME_HEADER: u8 = 2;
pub const FRAME_BODY: u8 = 3;
pub const FRAME_HEARTBEAT: u8 = 8;
pub const FRAME_END: u8 = 0xCE;

pub const CLASS_CONNECTION: u16 = 10;
pub const CLASS_CHANNEL: u16 = 20;
pub const CLASS_EXCHANGE: u16 = 40;
pub const CLASS_QUEUE: u16 = 50;
pub const CLASS_BASIC: u16 = 60;
pub const CLASS_CONFIRM: u16 = 85;
pub const CLASS_TX: u16 = 90;

pub const CONNECTION_START: u16 = 10;
pub const CONNECTION_START_OK: u16 = 11;
pub const CONNECTION_TUNE: u16 = 30;
pub const CONNECTION_TUNE_OK: u16 = 31;
pub const CONNECTION_OPEN: u16 = 40;
pub const CONNECTION_OPEN_OK: u16 = 41;
pub const CONNECTION_CLOSE: u16 = 50;
pub const CONNECTION_CLOSE_OK: u16 = 51;

pub const CHANNEL_OPEN: u16 = 10;
pub const CHANNEL_OPEN_OK: u16 = 11;
pub const CHANNEL_FLOW: u16 = 20;
pub const CHANNEL_FLOW_OK: u16 = 21;
pub const CHANNEL_CLOSE: u16 = 40;
pub const CHANNEL_CLOSE_OK: u16 = 41;

pub const EXCHANGE_DECLARE: u16 = 10;
pub const EXCHANGE_DECLARE_OK: u16 = 11;
pub const EXCHANGE_DELETE: u16 = 20;
pub const EXCHANGE_DELETE_OK: u16 = 21;
pub const EXCHANGE_BIND: u16 = 30;
pub const EXCHANGE_BIND_OK: u16 = 31;

pub const QUEUE_DECLARE: u16 = 10;
pub const QUEUE_DECLARE_OK: u16 = 11;
pub const QUEUE_BIND: u16 = 20;
pub const QUEUE_BIND_OK: u16 = 21;
pub const QUEUE_PURGE: u16 = 30;
pub const QUEUE_PURGE_OK: u16 = 31;
pub const QUEUE_DELETE: u16 = 40;
pub const QUEUE_DELETE_OK: u16 = 41;
pub const QUEUE_UNBIND: u16 = 50;
pub const QUEUE_UNBIND_OK: u16 = 51;

pub const BASIC_QOS: u16 = 10;
pub const BASIC_QOS_OK: u16 = 11;
pub const BASIC_CONSUME: u16 = 20;
pub const BASIC_CONSUME_OK: u16 = 21;
pub const BASIC_CANCEL: u16 = 30;
pub const BASIC_CANCEL_OK: u16 = 31;
pub const BASIC_PUBLISH: u16 = 40;
pub const BASIC_RETURN: u16 = 50;
pub const BASIC_DELIVER: u16 = 60;
pub const BASIC_GET: u16 = 70;
pub const BASIC_GET_OK: u16 = 71;
pub const BASIC_GET_EMPTY: u16 = 72;
pub const BASIC_ACK: u16 = 80;
pub const BASIC_REJECT: u16 = 90;
pub const BASIC_RECOVER: u16 = 110;
pub const BASIC_RECOVER_OK: u16 = 111;
pub const BASIC_NACK: u16 = 120;

pub const CONFIRM_SELECT: u16 = 10;
pub const CONFIRM_SELECT_OK: u16 = 11;

pub const REPLY_SUCCESS: u16 = 200;
pub const REPLY_NOT_FOUND: u16 = 404;
pub const REPLY_PRECONDITION_FAILED: u16 = 406;
pub const REPLY_FRAME_ERROR: u16 = 501;
pub const REPLY_SYNTAX_ERROR: u16 = 502;
pub const REPLY_CHANNEL_ERROR: u16 = 504;
pub const REPLY_NOT_ALLOWED: u16 = 530;
pub const REPLY_COMMAND_INVALID: u16 = 503;
pub const REPLY_NOT_IMPLEMENTED: u16 = 540;
pub const REPLY_UNEXPECTED_FRAME: u16 = 505;
pub const REPLY_NO_ROUTE: u16 = 312;

#[derive(Debug)]
pub struct Frame {
    pub kind: u8,
    pub channel: u16,
    pub payload: Vec<u8>,
}

pub async fn read_frame<R: AsyncRead + Unpin>(
    reader: &mut R,
    max_frame: u32,
) -> io::Result<Option<Frame>> {
    let mut header = [0u8; 7];
    match reader.read_exact(&mut header).await {
        Ok(_) => {}
        Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(err) => return Err(err),
    }
    let kind = header[0];
    let channel = u16::from_be_bytes([header[1], header[2]]);
    let size = u32::from_be_bytes([header[3], header[4], header[5], header[6]]);
    // frame-max counts the whole frame: 7-byte header + payload + terminator.
    if size as usize + 8 > max_frame as usize {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "frame exceeds negotiated maximum",
        ));
    }
    let mut payload = vec![0u8; size as usize];
    reader.read_exact(&mut payload).await?;
    let mut end = [0u8; 1];
    reader.read_exact(&mut end).await?;
    if end[0] != FRAME_END {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "missing frame terminator",
        ));
    }
    Ok(Some(Frame {
        kind,
        channel,
        payload,
    }))
}

pub async fn write_frame<W: AsyncWrite + Unpin>(
    writer: &mut W,
    kind: u8,
    channel: u16,
    payload: &[u8],
) -> io::Result<()> {
    writer.write_all(&[kind]).await?;
    writer.write_all(&channel.to_be_bytes()).await?;
    writer
        .write_all(&(payload.len() as u32).to_be_bytes())
        .await?;
    writer.write_all(payload).await?;
    writer.write_all(&[FRAME_END]).await?;
    Ok(())
}

pub fn method_payload(class_id: u16, method_id: u16, args: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(args.len() + 4);
    out.extend_from_slice(&class_id.to_be_bytes());
    out.extend_from_slice(&method_id.to_be_bytes());
    out.extend_from_slice(args);
    out
}

pub struct Parser<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Parser<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Parser { bytes, pos: 0 }
    }

    fn take(&mut self, len: usize) -> io::Result<&'a [u8]> {
        let end = self
            .pos
            .checked_add(len)
            .ok_or_else(|| syntax("length overflow"))?;
        if end > self.bytes.len() {
            return Err(syntax("truncated method arguments"));
        }
        let out = &self.bytes[self.pos..end];
        self.pos = end;
        Ok(out)
    }

    pub fn u8(&mut self) -> io::Result<u8> {
        Ok(self.take(1)?[0])
    }

    pub fn u16(&mut self) -> io::Result<u16> {
        let bytes = self.take(2)?;
        Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
    }

    pub fn u32(&mut self) -> io::Result<u32> {
        let bytes = self.take(4)?;
        Ok(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    pub fn u64(&mut self) -> io::Result<u64> {
        let bytes = self.take(8)?;
        let mut out = [0u8; 8];
        out.copy_from_slice(bytes);
        Ok(u64::from_be_bytes(out))
    }

    pub fn short(&mut self) -> io::Result<u16> {
        self.u16()
    }

    pub fn shortstr(&mut self) -> io::Result<String> {
        let len = self.u8()? as usize;
        let bytes = self.take(len)?;
        String::from_utf8(bytes.to_vec()).map_err(|_| syntax("string is not UTF-8"))
    }

    pub fn longstr_bytes(&mut self) -> io::Result<Vec<u8>> {
        let len = self.u32()? as usize;
        Ok(self.take(len)?.to_vec())
    }

    pub fn skip_table(&mut self) -> io::Result<()> {
        let len = self.u32()? as usize;
        self.take(len)?;
        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.pos == self.bytes.len()
    }

    pub fn finish(&self) -> io::Result<()> {
        if self.pos == self.bytes.len() {
            Ok(())
        } else {
            Err(syntax("trailing bytes in method arguments"))
        }
    }
}

fn syntax(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.to_string())
}

pub struct Encoder {
    bytes: Vec<u8>,
}

impl Encoder {
    pub fn new() -> Self {
        Encoder { bytes: Vec::new() }
    }

    pub fn u8(&mut self, value: u8) -> &mut Self {
        self.bytes.push(value);
        self
    }

    pub fn u16(&mut self, value: u16) -> &mut Self {
        self.bytes.extend_from_slice(&value.to_be_bytes());
        self
    }

    pub fn u32(&mut self, value: u32) -> &mut Self {
        self.bytes.extend_from_slice(&value.to_be_bytes());
        self
    }

    pub fn u64(&mut self, value: u64) -> &mut Self {
        self.bytes.extend_from_slice(&value.to_be_bytes());
        self
    }

    pub fn shortstr(&mut self, value: &str) -> &mut Self {
        let bytes = value.as_bytes();
        let len = bytes.len().min(255);
        self.bytes.push(len as u8);
        self.bytes.extend_from_slice(&bytes[..len]);
        self
    }

    pub fn longstr(&mut self, value: &[u8]) -> &mut Self {
        self.bytes
            .extend_from_slice(&(value.len() as u32).to_be_bytes());
        self.bytes.extend_from_slice(value);
        self
    }

    pub fn empty_table(&mut self) -> &mut Self {
        self.bytes.extend_from_slice(&0u32.to_be_bytes());
        self
    }

    pub fn table(&mut self, entries: &[(&str, TableValue<'_>)]) -> &mut Self {
        let encoded = encode_table(entries);
        self.bytes.extend_from_slice(&encoded);
        self
    }

    pub fn finish(self) -> Vec<u8> {
        self.bytes
    }
}

impl Default for Encoder {
    fn default() -> Self {
        Encoder::new()
    }
}

pub enum TableValue<'a> {
    String(&'a str),
    Bool(bool),
    Table(&'a [(&'a str, TableValue<'a>)]),
}

fn encode_table(entries: &[(&str, TableValue<'_>)]) -> Vec<u8> {
    let mut table = Encoder::new();
    for (key, value) in entries {
        table.shortstr(key);
        match value {
            TableValue::String(text) => {
                table.u8(b'S');
                table.longstr(text.as_bytes());
            }
            TableValue::Bool(flag) => {
                table.u8(b't');
                table.u8(if *flag { 1 } else { 0 });
            }
            TableValue::Table(inner) => {
                table.u8(b'F');
                let nested = encode_table(inner);
                table.bytes.extend_from_slice(&nested);
            }
        }
    }
    let mut out = Vec::with_capacity(table.bytes.len() + 4);
    out.extend_from_slice(&(table.bytes.len() as u32).to_be_bytes());
    out.extend_from_slice(&table.bytes);
    out
}

#[derive(Debug, Clone, Default)]
pub struct BasicProperties {
    pub content_type: Option<String>,
    pub delivery_mode: Option<u8>,
    pub correlation_id: Option<String>,
    pub reply_to: Option<String>,
    pub message_id: Option<String>,
}

const FLAG_CONTENT_TYPE: u16 = 0x8000;
const FLAG_CONTENT_ENCODING: u16 = 0x4000;
const FLAG_HEADERS: u16 = 0x2000;
const FLAG_DELIVERY_MODE: u16 = 0x1000;
const FLAG_PRIORITY: u16 = 0x0800;
const FLAG_CORRELATION_ID: u16 = 0x0400;
const FLAG_REPLY_TO: u16 = 0x0200;
const FLAG_EXPIRATION: u16 = 0x0100;
const FLAG_MESSAGE_ID: u16 = 0x0080;
const FLAG_TIMESTAMP: u16 = 0x0040;
const FLAG_TYPE: u16 = 0x0020;
const FLAG_USER_ID: u16 = 0x0010;
const FLAG_APP_ID: u16 = 0x0008;
const FLAG_CLUSTER_ID: u16 = 0x0004;

pub struct ContentHeader {
    pub body_size: u64,
    pub properties: BasicProperties,
}

pub fn parse_content_header(payload: &[u8]) -> io::Result<ContentHeader> {
    let mut parser = Parser::new(payload);
    let class_id = parser.u16()?;
    if class_id != CLASS_BASIC {
        return Err(syntax("content header for unsupported class"));
    }
    let _weight = parser.u16()?;
    let body_size = parser.u64()?;
    let flags = parser.u16()?;

    let mut properties = BasicProperties::default();
    if flags & FLAG_CONTENT_TYPE != 0 {
        properties.content_type = Some(parser.shortstr()?);
    }
    if flags & FLAG_CONTENT_ENCODING != 0 {
        parser.shortstr()?;
    }
    if flags & FLAG_HEADERS != 0 {
        parser.skip_table()?;
    }
    if flags & FLAG_DELIVERY_MODE != 0 {
        properties.delivery_mode = Some(parser.u8()?);
    }
    if flags & FLAG_PRIORITY != 0 {
        parser.u8()?;
    }
    if flags & FLAG_CORRELATION_ID != 0 {
        properties.correlation_id = Some(parser.shortstr()?);
    }
    if flags & FLAG_REPLY_TO != 0 {
        properties.reply_to = Some(parser.shortstr()?);
    }
    if flags & FLAG_EXPIRATION != 0 {
        parser.shortstr()?;
    }
    if flags & FLAG_MESSAGE_ID != 0 {
        properties.message_id = Some(parser.shortstr()?);
    }
    if flags & FLAG_TIMESTAMP != 0 {
        parser.u64()?;
    }
    if flags & FLAG_TYPE != 0 {
        parser.shortstr()?;
    }
    if flags & FLAG_USER_ID != 0 {
        parser.shortstr()?;
    }
    if flags & FLAG_APP_ID != 0 {
        parser.shortstr()?;
    }
    if flags & FLAG_CLUSTER_ID != 0 {
        parser.shortstr()?;
    }
    parser.finish()?;
    Ok(ContentHeader {
        body_size,
        properties,
    })
}

pub fn encode_content_header(properties: &BasicProperties, body_size: u64) -> Vec<u8> {
    let mut flags = 0u16;
    if properties.content_type.is_some() {
        flags |= FLAG_CONTENT_TYPE;
    }
    if properties.delivery_mode.is_some() {
        flags |= FLAG_DELIVERY_MODE;
    }
    if properties.correlation_id.is_some() {
        flags |= FLAG_CORRELATION_ID;
    }
    if properties.reply_to.is_some() {
        flags |= FLAG_REPLY_TO;
    }
    if properties.message_id.is_some() {
        flags |= FLAG_MESSAGE_ID;
    }

    let mut encoder = Encoder::new();
    encoder.u16(CLASS_BASIC).u16(0).u64(body_size).u16(flags);
    if let Some(value) = &properties.content_type {
        encoder.shortstr(value);
    }
    if let Some(value) = properties.delivery_mode {
        encoder.u8(value);
    }
    if let Some(value) = &properties.correlation_id {
        encoder.shortstr(value);
    }
    if let Some(value) = &properties.reply_to {
        encoder.shortstr(value);
    }
    if let Some(value) = &properties.message_id {
        encoder.shortstr(value);
    }
    encoder.finish()
}

pub fn channel_error_args(code: u16, text: &str, class_id: u16, method_id: u16) -> Vec<u8> {
    let mut encoder = Encoder::new();
    encoder
        .u16(code)
        .shortstr(text)
        .u16(class_id)
        .u16(method_id);
    encoder.finish()
}

pub fn connection_error_args(code: u16, text: &str, class_id: u16, method_id: u16) -> Vec<u8> {
    channel_error_args(code, text, class_id, method_id)
}

pub async fn write_method<W: AsyncWrite + Unpin>(
    writer: &mut W,
    channel: u16,
    class_id: u16,
    method_id: u16,
    args: &[u8],
) -> io::Result<()> {
    write_frame(
        writer,
        FRAME_METHOD,
        channel,
        &method_payload(class_id, method_id, args),
    )
    .await
}

pub async fn write_content<W: AsyncWrite + Unpin>(
    writer: &mut W,
    channel: u16,
    properties: &BasicProperties,
    body: &[u8],
    frame_max: usize,
) -> io::Result<()> {
    let header = encode_content_header(properties, body.len() as u64);
    write_frame(writer, FRAME_HEADER, channel, &header).await?;
    let chunk_size = frame_max.saturating_sub(8).max(1);
    for chunk in body.chunks(chunk_size) {
        write_frame(writer, FRAME_BODY, channel, chunk).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encoded_frame(payload_size: usize) -> std::io::Cursor<Vec<u8>> {
        let mut bytes = Vec::with_capacity(payload_size + 8);
        bytes.push(FRAME_BODY);
        bytes.extend_from_slice(&1u16.to_be_bytes());
        bytes.extend_from_slice(&(payload_size as u32).to_be_bytes());
        bytes.extend(std::iter::repeat_n(0xAAu8, payload_size));
        bytes.push(FRAME_END);
        std::io::Cursor::new(bytes)
    }

    #[tokio::test]
    async fn frame_max_counts_header_and_terminator() {
        let mut exact = encoded_frame(4088);
        let frame = read_frame(&mut exact, 4096).await.unwrap().unwrap();
        assert_eq!(frame.payload.len(), 4088);

        let mut oversized = encoded_frame(4089);
        assert!(read_frame(&mut oversized, 4096).await.is_err());
    }
}
