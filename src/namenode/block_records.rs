use std::collections::HashMap;

/// stores which datanodes each block is stored on
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
            block_mappings: HashMap::new(),
        }
    }

    /// Replaces block if it already exists based on the hashed block_id determines which
    /// datanode(s) to store on returns the IP address of a datanode to write to
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

    /// Removes a block form the records
    pub fn remove_block_from_records(&mut self, block_id: &u64) -> Option<Vec<String>> {
        self.block_mappings.remove(block_id)
    }

    /// Returns a list of datanodes that a block exists on
    pub fn get_block_datanodes(&self, block_id: &u64) -> Result<Vec<String>, &str> {
        match self.block_mappings.get(block_id) {
            Some(metadata) => Ok(metadata.clone()),
            None => Err("Block Not in Records"),
        }
    }
}
