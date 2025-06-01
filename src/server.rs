use std::{io::{Read, Write}, net::TcpListener, net::TcpStream};
use std::io::Cursor;
use crate::wire::{Request, Response};

pub struct Server {}

impl Server {
    pub fn serve(&self, ln: TcpListener) {
        for stream in ln.incoming() {
            match stream {
                Ok(stream) => {
                    println!("accepted new connection");
                    
                    handle_stream(stream);
                }
                Err(e) => {
                    println!("error: {}", e);
                }
            }
        }
    }
}

fn handle_stream(mut stream: TcpStream) {
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