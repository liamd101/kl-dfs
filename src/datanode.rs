#![allow(dead_code)]
use chrono::Utc;
use prost::Message;
use std::error::Error;
use storage::Storage;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::time::Duration;

use crate::proto::HeartbeatMessage;

/// Server that runs a datanode
pub struct DataNodeServer<'a> {
    /// TCP address of the datanode
    datanode_addr: String,

    /// Block storage of the datanode
    storage: Storage,

    /// Addresses of other datanodes
    peer_addrs: Vec<&'a str>,

    /// Connection to the namenode
    namenode_connection: Mutex<Option<TcpStream>>,
}

impl<'a> DataNodeServer<'a> {
    pub fn new(port: String) -> Self {
        let datanode_addr: String = format!("127.0.0.1:{}", port);
        DataNodeServer {
            datanode_addr,
            storage: Storage::new(),
            peer_addrs: vec![],
            namenode_connection: Mutex::new(None),
        }
    }

    pub async fn create_listener(&self) -> Result<TcpListener, Box<dyn Error>> {
        let listener = TcpListener::bind(&self.datanode_addr).await?;
        Ok(listener)
    }

    pub async fn bind_port(&self) -> Result<TcpStream, Box<dyn Error>> {
        let stream = TcpStream::connect(&self.datanode_addr).await?;
        Ok(stream)
    }

    pub async fn connect_to_namenode(&self, namenode_addr: &str) -> Result<(), Box<dyn Error>> {
        let stream = TcpStream::connect(namenode_addr).await?;
        let mut lock = self.namenode_connection.lock().await;
        *lock = Some(stream);
        Ok(())
    }

    pub async fn send_heartbeat_loop(&self) {
        let heartbeat_interval = Duration::from_secs(2);
        let mut interval = tokio::time::interval(heartbeat_interval);

        loop {
            interval.tick().await;
            match self.send_heartbeat().await {
                Ok(_) => println!("Sent heartbeat"),
                Err(e) => println!("Error sending heartbeat: {}", e),
            }
        }
    }

    pub async fn send_heartbeat(&self) -> Result<(), Box<dyn Error>> {
        let mut lock = self.namenode_connection.lock().await;

        if let Some(ref mut stream) = *lock {
            let now = Utc::now();
            let heartbeat = HeartbeatMessage {
                node_id: self.datanode_addr.clone(),
                timestamp: now.timestamp(),
            };

            let mut buf = Vec::new();
            heartbeat.encode(&mut buf)?;
            stream.write_all(&buf).await?;
        } else {
            return Err("Connection to namenode is not established".into());
        }
        Ok(())
    }

    pub async fn add_block(&mut self) {
        todo!()
    }

    pub fn add_peer(&mut self, _peer_addr: String) -> Result<(), Box<dyn Error>> {
        todo!()
    }
}

pub mod storage {
    use crate::block::Block;
    use std::path::PathBuf;

    // TODO: Move to config file or somewhere else
    const DATA_DIR: &str = "./data";

    /// Block storage for a datanode
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
            self.blocks.get(id)
        }
    }
}
