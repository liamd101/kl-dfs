#![allow(dead_code)]
use crate::block::Block;
use std::path::PathBuf;

const DATA_DIR: &str = "./data";

struct DataNode {
    id: usize,
    blocks: Vec<Block>,
    data_dir: PathBuf,
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
        let block = self.blocks.get(id);
        block
    }
}
