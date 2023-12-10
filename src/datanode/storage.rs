use crate::block::Block;
use std::{error::Error, path::PathBuf};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncWriteExt},
};

// TODO: Move to config file or somewhere else
const DATA_DIR: &str = "./data";

/// Block storage for a datanode
#[derive(Clone)]
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

    pub async fn read(&self, name: &str) -> Result<Vec<u8>, Box<dyn Error>> {
        if !self.exists(name) {
            return Err("Block does not exist".into());
        }
        let block = self.get_block(name).unwrap();
        let buffer = block.read();
        Ok(buffer)
    }

    pub async fn create(&mut self, name: &str) -> Result<(), Box<dyn Error>> {
        if self.exists(name) {
            return Err("Block already exists".into());
        }
        let block = Block::new(name.to_string(), vec![]);
        self.blocks.push(block);
        Ok(())
    }

    pub async fn update(&mut self, name: &str, mut data: Vec<u8>) -> Result<(), Box<dyn Error>> {
        Ok(())
    }

    pub async fn delete(&mut self, name: &str) -> Result<(), Box<dyn Error>> {
        self.blocks.retain(|b| b.name != name);
        Ok(())
    }

    fn get_block(&self, name: &str) -> Option<&Block> {
        self.blocks.iter().find(|b| b.name == name)
    }

    fn exists(&self, name: &str) -> bool {
        self.blocks.iter().any(|b| b.name == name)
    }
}
