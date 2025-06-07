use std::{io::{Cursor, Read}};
use byteorder::{ReadBytesExt, BigEndian};

pub trait Serializable {
    fn serialize(&self) -> Vec<u8>;
}

pub trait Deserializable {
    fn deserialize(&mut self,  reader: &mut Cursor<&[u8]>);
}

#[derive(Default)]
struct Int8 {
    value: i8
}

impl Serializable for Int8 {
    fn serialize(&self) -> Vec<u8> {
        let mut res = Vec::new();
        res.extend(self.value.to_be_bytes());

        res
    }
}

impl Deserializable for Int8 {
    fn deserialize(&mut self,  reader: &mut Cursor<&[u8]>) {
        self.value = reader.read_i8().unwrap();
    }
}

#[derive(Default)]
struct NullableString {
    length: i16,
    data: Vec<u8>
}

impl Deserializable for NullableString {
    fn deserialize(&mut self,  reader: &mut Cursor<&[u8]>) {

    }
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

impl Varint {
    fn deserialize(&mut self, reader: &mut Cursor<&[u8]>) {
        self.value = reader.read_i8().unwrap() - 1;
    }
}

#[derive(Default)]
pub struct CompactArray<T: Serializable + Deserializable + Default> {
    data: Option<Vec<T>>,
}

impl<T: Serializable + Deserializable + Default> CompactArray<T> {
    pub fn append(&mut self, elem: T) {
        match &mut self.data {
            Some(data) => data.push(elem),
            None => {}
        }
    }
}

impl <T: Serializable + Deserializable + Default> Deserializable for CompactArray<T> {
    fn deserialize(&mut self, reader: &mut Cursor<&[u8]>) {
        let mut len_varint = Varint::default();
        len_varint.deserialize(reader);

        for _i in 0..len_varint.value {
            let mut elem = T::default();
            elem.deserialize(reader);
            self.append(elem);
        }
    }
}

impl <T: Serializable + Deserializable + Default> Serializable for CompactArray<T> {
    fn serialize(&self) -> Vec<u8> {
        let mut res = Vec::new();

        match &self.data {
            Some(data) => {
                let len_varint = Varint{value: (data.len() + 1) as i8};
                res.extend(len_varint.serialize());
                for elem in data {
                    res.extend(elem.serialize());
                }
            }
            None => {
                let len_varint = Varint{value: 0};
                res.extend(len_varint.serialize());
            }
        }

        res
    }
}

#[derive(Default)]
pub struct CompactString {
    data: String
}

impl Deserializable for CompactString {
    fn deserialize(&mut self, reader: &mut Cursor<&[u8]>) {
        let string_len = reader.read_i8().unwrap();

        let mut buf = vec![0u8; string_len as usize];
        let _ = reader.read_exact(&mut buf);

        self.data = String::from_utf8(buf).unwrap();
    }
}

impl Serializable for CompactString {
    fn serialize(&self) -> Vec<u8> {
        let mut res = Vec::new();

        let len_varint = Varint{value: (self.data.len() + 1) as i8};

        res.extend(len_varint.serialize());
        res.extend(self.data.as_bytes());

        res
    }
}

#[derive(Default)]
pub struct RequestHeader {
    pub request_api_key: i16,
    pub request_api_version: i16,
    pub correlation_id: i32,
    client_id: NullableString,
    tag_buffer: CompactArray<Int8>
}

impl RequestHeader {
    pub fn deserialize(&mut self, reader: &mut Cursor<&[u8]>) {
        self.request_api_key = reader.read_i16::<BigEndian>().unwrap();
        self.request_api_version = reader.read_i16::<BigEndian>().unwrap();
        self.correlation_id = reader.read_i32::<BigEndian>().unwrap();
        self.client_id.deserialize(reader);
        self.tag_buffer.deserialize(reader);
    }
}

#[derive(Default)]
pub struct Request {
    pub header: RequestHeader,
    pub body: Vec<u8>
}

impl Request {
    pub fn deserialize(&mut self, reader: &mut Cursor<&[u8]>) {
        self.header.deserialize(reader);
    }
}

pub struct DescribeTopicPartitionsRequest {
    pub topics: CompactArray<CompactString>,
    pub partitions_limit: i32,
    pub cursor: u8,
    pub tag_buffer: CompactArray<Int8>,
}

impl DescribeTopicPartitionsRequest {
    pub fn deserialize(&mut self, reader: &mut Cursor<&[u8]>) {
        self.topics.deserialize(reader);
        self.partitions_limit = reader.read_i32::<BigEndian>().unwrap();
        self.cursor = reader.read_u8().unwrap();
        self.tag_buffer.deserialize(reader);
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
pub struct ApiVersion {
    pub key: i16,
    pub min: i16,
    pub max: i16,
    pub tag_buffer: CompactArray<Int8>,
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

impl Deserializable for ApiVersion {
    fn deserialize(&mut self,  reader: &mut Cursor<&[u8]>) {
        self.key = reader.read_i16::<BigEndian>().unwrap();
        self.min = reader.read_i16::<BigEndian>().unwrap();
        self.max = reader.read_i16::<BigEndian>().unwrap();
        self.tag_buffer.deserialize(reader);
    }
}

pub struct ApiVersionsResponse {
    pub error_code: i16,
    pub versions: CompactArray<ApiVersion>,
    pub throttle_time_ms: i32,
    pub tag_buffer: CompactArray<Int8>
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

pub struct DescribeTopicPartitionsResponse {

}

impl DescribeTopicPartitionsResponse {
    pub fn serialize(&self) -> Vec<u8> {

    }
}