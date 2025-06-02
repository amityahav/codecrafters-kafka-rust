mod wire;
mod server;

use server::Server;
use std::{f32::consts::LN_10, net::TcpListener};

fn main() {
    let ln = TcpListener::bind("127.0.0.1:9092").unwrap();

    let srv = Server{};
    srv.serve(ln);
}