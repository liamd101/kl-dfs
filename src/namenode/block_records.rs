// stores which files/blocks are on which datanodes
use std::collections::HashMap;

pub struct BlockRecords {
    /// Mapping from block id to datanode addrs
    block_mappings: HashMap<u64, Vec<String>>,
}

impl Default for BlockRecords {
    fn default() -> Self {
        Self::new()
    }
}

impl BlockRecords {
    pub fn new() -> Self {
        Self {
            block_mappings: HashMap::new(), // mapping from block id to metadata
        }
    }

    // replaces block if it already exists
    // based on the block_id (which is a hash), determines which datanode(s) to store on
    // returns the IP address of a datanode to write to
    pub fn add_block_to_records(
        &mut self,
        block_id: u64,
        datanode_addrs: Vec<String>,
    ) -> Result<Vec<String>, &str> {
        match self.block_mappings.get(&block_id) {
            Some(addrs) => Ok(addrs.clone()),
            None => {
                self.block_mappings.insert(block_id, datanode_addrs.clone());
                Ok(datanode_addrs.clone())
            }
        }
    }

    pub fn remove_block_from_records(&mut self, block_id: &u64) -> Option<Vec<String>> {
        self.block_mappings.remove(block_id)
    }

    // returns Block Metadata from block_id - right now is same as file_id
    pub fn get_block_metadata(&self, block_id: &u64) -> Option<&Vec<String>> {
        self.block_mappings.get(block_id)
    }

    pub fn add_block_replicate(
        &mut self,
        block_id: &u64,
        datanode_addr: String,
    ) -> Result<(), &str> {
        match self.block_mappings.get_mut(block_id) {
            Some(metadata) => {
                metadata.push(datanode_addr.clone());
                Ok(())
            }
            None => Err("Block Not in Records"),
        }
    }

    // returns a list of datanodes that a block exists on
    pub fn get_block_datanodes(&self, block_id: &u64) -> Result<Vec<String>, &str> {
        match self.block_mappings.get(block_id) {
            Some(metadata) => Ok(metadata.clone()),
            None => Err("Block Not in Records"),
        }
    }
}
