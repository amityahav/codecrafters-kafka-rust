use std::{io::{Read, Write}, net::TcpListener, net::TcpStream};
use std::io::Cursor;
use crate::wire::{ApiVersion, ApiVersionsResponse, CompactArray, Request, Response, TagBuffer, Serializable};

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
                    eprintln!("error: {}", e);
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

    let body_buf: Vec<u8>;

    match request.header.request_api_key {
        18 => {
            match handle_api_versions_req(&request) {
                Ok(res) => {
                   body_buf = res.serialize()
                },
                Err(e) => {
                    eprintln!("Failed handling ApiVersions request: {}", e);
                    return; // consider returning an error to the client
                }
            }
            
        },
        i16::MIN..=17_i16 | 19_i16..=i16::MAX => todo!()
    }

    // send back response.
    let res = Response{
        header: request.header.correlation_id,
        body: body_buf,
    };

    let _ = stream.write(&res.serialize());
}

fn handle_api_versions_req(_request: &Request) -> Result<ApiVersionsResponse, String>{
    let mut versions: CompactArray<ApiVersion> = CompactArray::default(); 
    versions.append(ApiVersion{
        key: 18,
        min: 0,
        max: 4,
        tag_buffer:TagBuffer{data:0},
    });

    Ok(ApiVersionsResponse{
        error_code: 0,
        versions,
        throttle_time_ms: 0,
        tag_buffer: TagBuffer { data: 0 }
    })
}