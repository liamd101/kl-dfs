use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::prelude::*;

use std::collections::HashMap;

// use crate::proto::network_comms;
use network_comms::{};

struct FileMetadata {
    file_id: u64, // Hash(filename || owner)
    file_name: String,
    size: u64,
    owner: i64,

}

pub struct NameNodeServer<'a> {
    datanodes: Vec<DataNode>,
    num_datanodes: i64,
    blocks: HashMap<u64, Vec<DataNode>>,
    metadata: HashMap<u64, FileMetadata>, // from file id : file metadata
    addr: &'a str,
}

impl<'a> NameNodeServer<'a> {
    pub fn new(port: String) -> Self {
        NameNodeServer {
            datanodes: Vec::new(),
            num_datanodes: 0,
            blocks: HashMap::new(),
            metadata: HashMap::new(),
            addr: format!("127.0.0.1:{}", port),
        }
    }

    pub fn run_nameserver(&self) {
        // match TcpListener::bind(&self.addr).await.unwrap() {
        //     Ok(_) => {
        //         println!("nameserver bound to {}", &self.addr);
        //     }
        //     Err(err) => {
        //         println("error binding client");
        //         Err(err)
        //     }
        // }
        let listener = TcpListener::bind(&self.addr).await.unwrap();

        loop {
            match listener.accept().await {
                Ok((socket, _)) => {
                    tokio::spawn(handle_connection(socket));
                }
                Err(err) => {
                    eprintln!("Error accepting connection: {}", err);
                }
            }
        }

    }

}