#![allow(dead_code)]
use crate::block::Block;
use std::path::PathBuf;

// TODO: Move to config file
const DATA_DIR: &str = "./data";

struct DataNode {
    /// ID of datanode stored in namenode
    id: usize,

    /// Blocks stored in datanode
    blocks: Vec<Block>,

    /// Directory where blocks are stored
    data_dir: PathBuf,

    /// Port to listen on
    port: u16,
}

impl DataNode {
    fn new(id: usize) -> Self {
        let data_dir = PathBuf::from(DATA_DIR);
        DataNode {
            id,
            data_dir,
            blocks: vec![],
            port: 8000,
        }
    }

    fn add_block(id: usize, data: Option<Vec<u8>>) -> Block {
        let data = match data {
            Some(data) => data,
            None => vec![],
        };
        Block::new(id, data)
    }

    fn read_block(&self, id: usize) -> Option<&Block> {
        self.blocks.get(id)
    }
}
