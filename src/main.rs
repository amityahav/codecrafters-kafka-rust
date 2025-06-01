#![allow(unused_imports)]
use std::{io::Write, net::TcpListener};

struct Response {
    Header: i32, // correlation-id
    Body: Vec<u8>
}

impl Response {
    fn serialize(&self) -> Vec<u8> {
        let mut res: Vec<u8> = Vec::new();

        let message_size: i32 = 5; // some random number for now
        res.extend(message_size.to_be_bytes());
        res.extend(self.Header.to_be_bytes());
        res.extend(self.Body.clone());

        res
    }
}

fn main() {
    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();
    
    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                println!("accepted new connection");

                let res = Response{
                    Header: 7,
                    Body: Vec::new(),
                };

                let _ = stream.write(&res.serialize());
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
