use std::io::Cursor;
use byteorder::{ReadBytesExt, BigEndian};

#[derive(Default)]
struct NullableString {
    length: i16,
    data: Vec<u8>
}

#[derive(Default)]
struct CompactArray {
    
}

#[derive(Default)]
pub struct RequestHeader {
    pub request_api_key: i16,
    request_api_version: i16,
    pub correlation_id: i32,
    client_id: NullableString,
    tag_buffer: CompactArray
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

impl Response {
    pub fn serialize(&self) -> Vec<u8> {
        let mut res: Vec<u8> = Vec::new();

        let body_size = self.body.len() as i32;
        let message_size: i32 = 4 + body_size;
        res.extend(message_size.to_be_bytes());
        res.extend(self.header.to_be_bytes());
        res.extend(self.body.clone());

        res
    }
}

pub struct ApiVersionsResponse {
    pub error_code: i16
}

impl ApiVersionsResponse {
    pub fn serialize(&self) -> Vec<u8> {
        let mut res: Vec<u8> = Vec::new();

        res.extend(self.error_code.to_be_bytes());

        res 
    }
}