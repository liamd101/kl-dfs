#![allow(dead_code)]
use std::error::Error;
use storage::Storage;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};

pub struct DataNodeServer {
    server_addr: String,
    storage: Storage,
    peer_addrs: Vec<String>,
}

impl DataNodeServer {
    pub fn new(port: String) -> Self {
        let server_addr = format!("127.0.0.1:{port}");
        DataNodeServer {
            server_addr,
            storage: Storage::new(),
            peer_addrs: vec![],
        }
    }

    pub async fn create_listener(&self) -> Result<TcpListener, Box<dyn Error>> {
        let listener = TcpListener::bind(&self.server_addr).await?;
        Ok(listener)
    }

    pub async fn bind_port(&self) -> Result<TcpStream, Box<dyn Error>> {
        let mut stream = TcpStream::connect(&self.server_addr).await?;
        stream.write_all(b"hello world").await?;
        Ok(stream)
    }

    pub async fn add_block(&mut self) {
        todo!()
    }

    pub fn add_peer(&mut self, _peer_addr: String) -> Result<(), Box<dyn Error>> {
        todo!()
    }

    pub async fn send_heartbeat(&self) -> Result<(), Box<dyn Error>> {
        todo!()
    }
}

pub mod storage {
    use crate::block::Block;
    use std::path::PathBuf;

    // TODO: Move to config file or somewhere else
    const DATA_DIR: &str = "./data";

    pub struct Storage {
        /// Blocks stored in datanode
        pub blocks: Vec<Block>,

        /// Directory where blocks are stored
        data_dir: PathBuf,
    }

    impl Default for Storage {
        fn default() -> Self {
            Self::new()
        }
    }

    impl Storage {
        pub fn new() -> Self {
            let data_dir = PathBuf::from(DATA_DIR);
            Storage {
                data_dir,
                blocks: vec![],
            }
        }

        pub fn add_block(&mut self, data: Option<Vec<u8>>) -> Block {
            let data = match data {
                Some(data) => data,
                None => vec![],
            };
            let block = Block::new(self.blocks.len(), data);
            self.blocks.push(block.clone());
            block
        }

        pub fn get_block(&self, id: usize) -> Option<&Block> {
            match self.blocks.get(id) {
                Some(block) => Some(block),
                None => None,
            }
        }
    }
}
