use crate::block::Block;
use std::error::Error;

/// Block storage for a datanode
#[derive(Clone)]
pub struct Storage {
    /// Blocks stored in datanode
    pub blocks: Vec<Block>,
}

impl Default for Storage {
    fn default() -> Self {
        Self::new()
    }
}

impl Storage {
    pub fn new() -> Self {
        Storage { blocks: vec![] }
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

    pub async fn update(&mut self, name: &str, data: Vec<u8>) -> Result<(), Box<dyn Error>> {
        let block = self.get_block_mut(name).unwrap();
        block.write(data);
        Ok(())
    }

    pub async fn delete(&mut self, name: &str) -> Result<(), Box<dyn Error>> {
        self.blocks.retain(|b| b.name != name);
        Ok(())
    }

    fn get_block_mut(&mut self, name: &str) -> Option<&mut Block> {
        self.blocks.iter_mut().find(|b| b.name == name)
    }

    fn get_block(&self, name: &str) -> Option<&Block> {
        self.blocks.iter().find(|b| b.name == name)
    }

    fn exists(&self, name: &str) -> bool {
        self.blocks.iter().any(|b| b.name == name)
    }
}
