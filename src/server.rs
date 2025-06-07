use std::{io::{ErrorKind, Read, Write}, net::{TcpListener, TcpStream}};
use std::io::Cursor;
use std::thread;

use crate::wire::{ApiVersion, ApiVersionsResponse, DescribeTopicPartitionsResponse, CompactArray, Request, Response, Serializable};

pub struct Server {}

impl Server {
    pub fn serve(&self, ln: TcpListener) {
        for stream in ln.incoming() {
            match stream {
                Ok(stream) => {
                    println!("accepted new connection");
                    
                    thread::spawn(|| {
                        match handle_stream(stream) {
                            Ok(()) => {},
                            Err(e) => eprintln!("{}", e),
                        }
                    });
                }
                Err(e) => {
                    eprintln!("error: {}", e);
                }
            }
        }
    }
}

const API_VERSIONS: i16 = 18;
const DESCRIBE_TOPIC_PARTITIONS: i16 = 75;

fn apply_handler(request: &Request) -> Result<Vec<u8>, String> {
    match request.header.request_api_key {
        API_VERSIONS => {
            match handle_api_versions_req(request) {
                Ok(res) => Ok(res.serialize()),
                Err(e) => Err(format!("Failed handling ApiVersions request: {}", e))
            }
        },
        DESCRIBE_TOPIC_PARTITIONS => {
            match handle_describe_topic_partitions_req(request) {
                Ok(res) => Ok(res.serialize()),
                Err(e) => Err(format!("Failed handling DescribeTopicPartitions request: {}", e))
            }
        }

        i16::MIN..=17_i16 | 19_i16..=i16::MAX => todo!()
    }
}

fn handle_stream(mut stream: TcpStream) -> Result<(), String> {
    loop {
        // read message_size.
        let mut size_buf: [u8; 4] = [0; 4];
        if let Err(e) =  stream.read_exact(&mut size_buf) {
            if e.kind() == ErrorKind::UnexpectedEof {
                // client is closed
                return Ok(());
            }

            return Err(e.to_string());
        }

        let message_size = u32::from_be_bytes(size_buf) as usize;

        // read request.
        let mut request_buf = vec![0u8; message_size];
        let _ = stream.read_exact(&mut request_buf);

        // deserialize request header.
        let mut request = Request::default();
        request.header.deserialize(&mut Cursor::new(&request_buf));

        match apply_handler(&request) {
            Ok(res) => {
                // send back response.
                let res = Response{
                    header: request.header.correlation_id,
                    body: res,
                };

                let _ = stream.write(&res.serialize());
            },
            Err(e) => {
                return Err(format!("failed to apply handler: {}", e));
            }
        }
    }

    // print!("RES: ");
    // for byte in res.serialize() {
    //     print!("{:02x} ", byte);
    // }

    // println!();

}

fn handle_api_versions_req(request: &Request) -> Result<ApiVersionsResponse, String>{
    let mut versions: CompactArray<ApiVersion> = CompactArray::default();
    let mut error_code = 0;
    if !(0..=4).contains(&request.header.request_api_version) {
        error_code = 35;
    }

    versions.append(ApiVersion{
        key: API_VERSIONS,
        min: 0,
        max: 4,
        tag_buffer:CompactArray::default(),
    });

    versions.append(ApiVersion{
        key: DESCRIBE_TOPIC_PARTITIONS,
        min: 0,
        max: 4,
        tag_buffer:CompactArray::default(),
    });

    Ok(ApiVersionsResponse{
        error_code,
        versions,
        throttle_time_ms: 0,
        tag_buffer:CompactArray::default(),
    })
}

fn handle_describe_topic_partitions_req(req: &Request) -> Result<DescribeTopicPartitionsResponse, String> {

}