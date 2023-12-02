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

    pub async fn read(&mut self, name: &str) -> Result<Vec<u8>, Box<dyn Error>> {
        if !self.exists(name) {
            return Err("Block does not exist".into());
        }
        let block_path = self.data_dir.join(name);
        let mut file = File::open(block_path).await?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).await?;
        Ok(buffer)
    }

    pub async fn write(&mut self, name: &str, mut data: Vec<u8>) -> Result<(), Box<dyn Error>> {
        let blockpath = self.data_dir.join(name);
        let mut file = if self.exists(name) {
            File::open(blockpath).await?
        } else {
            File::create(blockpath).await?
        };
        file.write_all(&mut data).await?;
        Ok(())
    }

    pub async fn delete(&mut self, name: &str) -> Result<(), Box<dyn Error>> {
        if !self.exists(name) {
            return Ok(());
        }
        let block_path = self.data_dir.join(name);
        tokio::fs::remove_file(block_path).await?;
        Ok(())
    }

    fn exists(&self, name: &str) -> bool {
        self.data_dir.join(name).exists() && self.blocks.iter().any(|b| b.name == name)
    }
}
