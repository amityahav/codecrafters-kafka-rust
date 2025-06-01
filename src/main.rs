#![allow(unused_imports)]
use std::{io::{Read, Write}, net::TcpListener};
use std::io::Cursor;
use byteorder::{ReadBytesExt, BigEndian, LittleEndian};

struct Response {
    header: i32, // correlation-id
    body: Vec<u8>
}

impl Response {
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
struct NullableString {
    length: i16,
    data: Vec<u8>
}
#[derive(Default)]
struct CompactArray {
    
}
#[derive(Default)]
struct RequestHeader {
    request_api_key: i16,
    requiest_api_version: i16,
    correlation_id: i32,
    client_id: NullableString,
    tag_buffer: CompactArray
}

impl RequestHeader {
    fn deserialize(&mut self, mut reader: Cursor<&[u8]>) {
        self.request_api_key = reader.read_i16::<BigEndian>().unwrap();
        self.requiest_api_version = reader.read_i16::<BigEndian>().unwrap();
        self.correlation_id = reader.read_i32::<BigEndian>().unwrap();
        // .. todo other fields
    }
}

#[derive(Default)]
struct Request {
    header: RequestHeader,
    body: Vec<u8>
}

impl Request {
    fn deserialize(&mut self, reader: Cursor<&[u8]>) {
        self.header.deserialize(reader);
    }
}

fn main() {
    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();
    
    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                println!("accepted new connection");
                
                // read message_size.
                let mut size_buf: [u8; 4] = [0; 4];
                let _ = stream.read_exact(&mut size_buf);
                let message_size = u32::from_be_bytes(size_buf) as usize;

                // read request.
                let mut request_buf = vec![0u8; message_size];
                let _ = stream.read_exact(&mut request_buf);

                // deserialize request.
                let mut request = Request::default();
                request.deserialize(Cursor::new(&request_buf));

                // send back response.
                let res = Response{
                    header: request.header.correlation_id,
                    body: Vec::new(),
                };

                let _ = stream.write(&res.serialize());
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
