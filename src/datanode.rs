#![allow(dead_code)]
use chrono::Utc;
use prost::Message;
use std::error::Error;
use std::net::{SocketAddr, IpAddr, Ipv4Addr};
use storage::Storage;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::time::Duration;

use crate::proto::HeartbeatMessage;

/// Server that runs a datanode
pub struct DataNodeServer {
    /// TCP address of the datanode
    datanode_addr: SocketAddr,

    /// Block storage of the datanode
    storage: Storage,

    /// Connection to the namenode
    namenode_addr: SocketAddr,
}

impl DataNodeServer {
    pub fn new(port: u16) -> Self {
        let datanode_addr: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port);
        let namenode_addr: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 3000);
        DataNodeServer {
            datanode_addr,
            storage: Storage::new(),
            namenode_addr
        }
    }

    pub async fn run_dataserver(&self) -> Result<(), Box<dyn std::error::Error>> {
        todo!()
    }

    pub async fn send_heartbeat(&self) -> Result<(), Box<dyn Error>> {
        Ok(())
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
