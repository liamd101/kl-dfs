use crate::block::Block;
use crate::proto::BlockInfo;
use std::error::Error;

/// Block storage for a datanode
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

    /// Takes a block name and returns the bytes stored in that block
    pub async fn read(&self, name: &str) -> Result<Vec<u8>, Box<dyn Error>> {
        if !self.exists(name) {
            return Err("Block does not exist".into());
        }
        let block = self.get_block(name).unwrap();
        let buffer = block.read();
        Ok(buffer)
    }

    /// Takes a block name and the bytes to be written and stores the block
    pub async fn create(
        &mut self,
        name: &str,
        block_info: BlockInfo,
    ) -> Result<(), Box<dyn Error>> {
        if self.exists(name) {
            return Err("Block already exists".into());
        }

        let data_to_write = if block_info.block_data.len() > block_info.block_size as usize {
            &block_info.block_data[..block_info.block_size as usize]
        } else {
            &block_info.block_data
        };

        let mut data = Vec::<u8>::with_capacity(block_info.block_size as usize);
        data.extend_from_slice(data_to_write);
        let block = Block::new(name.to_string(), data);
        self.blocks.push(block);
        Ok(())
    }

    /// Takes a block name and the bytes to be written and updates or creates the block
    pub async fn update(
        &mut self,
        name: &str,
        block_info: BlockInfo,
    ) -> Result<(), Box<dyn Error>> {
        if !self.exists(name) {
            self.create(name, block_info).await?;
        } else {
            let block = self.get_block_mut(name).unwrap();
            block.write(block_info);
        }
        Ok(())
    }

    /// Takes a block name and deletes the block
    pub async fn delete(&mut self, name: &str) -> Result<(), Box<dyn Error>> {
        self.blocks.retain(|b| b.name != name);
        Ok(())
    }

    /// Returns a mutable references to blocks with a given name
    fn get_block_mut(&mut self, name: &str) -> Option<&mut Block> {
        self.blocks.iter_mut().find(|b| b.name == name)
    }

    /// Returns a references to blocks with a given name
    fn get_block(&self, name: &str) -> Option<&Block> {
        self.blocks.iter().find(|b| b.name == name)
    }

    /// Returns true if a block with a given name exists
    fn exists(&self, name: &str) -> bool {
        self.blocks.iter().any(|b| b.name == name)
    }
}
