use std::io::Cursor;
use byteorder::{ReadBytesExt, BigEndian};

pub trait Serializable {
    fn serialize(&self) -> Vec<u8>;
}

#[derive(Default)]
struct NullableString {
    length: i16,
    data: Vec<u8>
}

fn zigzag_encode(n: i8) -> u8 {
    ((n << 1) ^ (n >> 7)) as u8
}

fn zigzag_decode(n: u32) -> i32 {
    ((n >> 1) as i32) ^ (-((n & 1) as i32))
}

#[derive(Default)]
struct Varint {
    value: i8
}

impl Serializable for Varint {
    fn serialize(&self) -> Vec<u8> {
        self.value.to_be_bytes().to_vec()
    }
}

#[derive(Default)]
pub struct CompactArray<T: Serializable> {
    data: Vec<T>,
}

impl<T: Serializable> CompactArray<T> {
    pub fn append(&mut self, elem: T) {
        self.data.push(elem);
    }
}

impl <T: Serializable> Serializable for CompactArray<T> {
    fn serialize(&self) -> Vec<u8> {
        let mut res = Vec::new();

        let len_varint = Varint{value: (self.data.len() + 1) as i8};

        res.extend(len_varint.serialize());
        for elem in &self.data {
            res.extend(elem.serialize());
        }

        res
    }
}

#[derive(Default)]
pub struct RequestHeader {
    pub request_api_key: i16,
    pub request_api_version: i16,
    pub correlation_id: i32,
    client_id: NullableString,
    //tag_buffer: CompactArray<String>
}

impl RequestHeader {
    pub fn deserialize(&mut self, mut reader: Cursor<&[u8]>) {
        self.request_api_key = reader.read_i16::<BigEndian>().unwrap();
        self.request_api_version = reader.read_i16::<BigEndian>().unwrap();
        self.correlation_id = reader.read_i32::<BigEndian>().unwrap();
        // .. todo other fields
    }
}

#[derive(Default)]
pub struct Request {
    pub header: RequestHeader,
    pub body: Vec<u8>
}

impl Request {
    pub fn deserialize(&mut self, reader: Cursor<&[u8]>) {
        self.header.deserialize(reader);
    }
}
pub struct Response {
    pub header: i32, // correlation-id
    pub body: Vec<u8>
}

impl Serializable for Response {
    fn serialize(&self) -> Vec<u8> {
        let mut res: Vec<u8> = Vec::new();

        let body_size = self.body.len() as i32;
        let message_size: i32 = 4 + body_size;
        res.extend(message_size.to_be_bytes());
        res.extend(self.header.to_be_bytes());
        res.extend(self.body.clone());

        res
    }
}

#[derive(Default)]
pub struct TagBuffer {
    pub data: u8
}

impl Serializable for TagBuffer {
    fn serialize(&self) -> Vec<u8> {
        // return 0 for now
        let mut res = Vec::new();

        res.extend(0_u8.to_be_bytes());

        res
    }
}

#[derive(Default)]
pub struct ApiVersion {
    pub key: i16,
    pub min: i16,
    pub max: i16,
    pub tag_buffer: TagBuffer
}

impl Serializable for ApiVersion {
    fn serialize(&self) -> Vec<u8> {
        let mut res = Vec::new();

        res.extend(self.key.to_be_bytes());
        res.extend(self.min.to_be_bytes());
        res.extend(self.max.to_be_bytes());
        res.extend(self.tag_buffer.serialize());

        res
    }
}

pub struct ApiVersionsResponse {
    pub error_code: i16,
    pub versions: CompactArray<ApiVersion>,
    pub throttle_time_ms: i32,
    pub tag_buffer: TagBuffer
}

impl ApiVersionsResponse {
    pub fn serialize(&self) -> Vec<u8> {
        let mut res: Vec<u8> = Vec::new();

        res.extend(self.error_code.to_be_bytes());
        res.extend(self.versions.serialize());
        res.extend(self.throttle_time_ms.to_be_bytes());
        res.extend(self.tag_buffer.serialize());

        res 
    }
}