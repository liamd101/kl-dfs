use std::collections::{HashMap, HashSet};
use std::sync::{atomic, Mutex, RwLock};
use std::time::{Duration, Instant};
use crate::namenode::block_records::BlockRecords;
// for atomic counter for id generation
// use std::sync::atomic::{AtomicUsize, Ordering};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

#[derive(Clone)]
pub struct DataNodeInfo {
    pub addr: String,
    pub alive: bool,
}

// basically recordkeeper/bookkeeper
pub struct NameNodeRecords {
    datanodes: Mutex<HashMap<String, DataNodeInfo>>, // datanode ip string, datanode info
    block_records: RwLock<BlockRecords>, // maps blocks to block metadata (including which datanodes a block is on)
    // block_id_counter: AtomicUsize,
    default_block_size: u64,
    
}

// TODO: heartbeat monitor - sends and checks for heartbeats and keeps datanodes updated with alive statuses
// how does it handle if we started making a file, but it wasn't actually written??
impl NameNodeRecords {
    pub fn new() -> Self {
        Self {
            datanodes: Mutex::new(HashMap::new()),
            block_records: RwLock::new(BlockRecords::new()),
            // block_id_counter: AtomicUsize::new(0),
            default_block_size: 4096
        }
    }

    pub async fn get_datanode_statuses(&self) -> Vec<DataNodeInfo> {
        let datanodes = self.datanodes.lock().unwrap();
        let statuses = datanodes.values().cloned().collect();
        statuses
    }

    // returns hash(file_name || uid)
    fn get_file_id(file_name: &str, uid: i64) -> u64 {
        let mut hasher = DefaultHasher::new();
        (file_name, uid).hash(&mut hasher);
        hasher.finish()
    }

    // Adds the new file block to block_records
    pub async fn add_file(&self, file_path: &str, owner: i64) -> Result<(), &str> {
        let file_id = Self::get_file_id(file_path, owner);
        let mut block_records = self.block_records.write().unwrap();
        match block_records.add_block_to_records(file_id) {
            Ok(()) => Ok(()),
            Err(_err) => Err("Block Already exists")
        }
    }

    // Removes a file block from block_records
    pub async fn remove_file(&self, file_path: &str, owner: i64) -> Result<(), &str> {
        let file_id = Self::get_file_id(file_path, owner);
        let mut block_records = self.block_records.write().unwrap();
        match block_records.remove_block_from_records(&file_id) {
            Ok(()) => Ok(()),
            Err(_err) => Err("Block Already exists")
        }
    }
    
    // returns a vector of datanode addresses that the file lives on
    pub async fn get_file_address(&self, file_path: &str, owner: i64) -> Result<Vec<String>, &str> {
        let file_id = Self::get_file_id(file_path, owner);
        let block_records = self.block_records.read().unwrap();

        match block_records.get_block_datanodes(&file_id) {
            Ok(datanodes) => Ok(datanodes),
            Err(_err) => Err("Block Not in Records")
        }
    }

}