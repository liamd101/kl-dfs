use crate::namenode::block_records::BlockRecords;
use std::collections::HashMap;
use std::sync::{atomic, Mutex, RwLock};
// for atomic counter for id generation
use std::collections::hash_map::DefaultHasher;
use std::error::Error;
use std::hash::{Hash, Hasher};
use std::sync::atomic::AtomicUsize;
use std::time::SystemTime;

const DEFAULT_BLOCK_SIZE: u64 = 4096;

#[derive(Clone)]
pub struct DataNodeInfo {
    _id: u64,
    pub addr: String,
    pub alive: bool,
}

// basically recordkeeper/bookkeeper
pub struct NameNodeRecords {
    datanodes: Mutex<HashMap<u64, DataNodeInfo>>, // datanode id : datanode info
    datanode_ids: Mutex<HashMap<String, u64>>,    // datanode ip string, datanode id
    block_records: RwLock<BlockRecords>, // maps blocks to block metadata (including which datanodes a block is on)
    datanode_id_counter: AtomicUsize,
    // default_block_size: u64,
    heartbeat_records: Mutex<HashMap<String, SystemTime>>, // map from datanode ip address to time of last message
}

impl Default for NameNodeRecords {
    fn default() -> Self {
        Self::new()
    }
}

// TODO: heartbeat monitor - sends and checks for heartbeats and keeps datanodes updated with alive statuses
// how does it handle if we started making a file, but it wasn't actually written??
impl NameNodeRecords {
    pub fn new() -> Self {
        Self {
            datanodes: Mutex::new(HashMap::new()),
            datanode_ids: Mutex::new(HashMap::new()),
            block_records: RwLock::new(BlockRecords::new()),
            datanode_id_counter: AtomicUsize::new(0),
            // default_block_size: 4096,
            heartbeat_records: Mutex::new(HashMap::new()),
        }
    }

    pub async fn get_datanode_statuses(&self) -> Vec<DataNodeInfo> {
        let datanodes = self.datanodes.lock().unwrap();
        let statuses = datanodes.values().cloned().collect();
        statuses
    }

    // returns hash(file_name || uid)
    fn get_file_id(file_name: &str) -> u64 {
        // get file_id from hashing
        let mut hasher = DefaultHasher::new();
        file_name.hash(&mut hasher);
        hasher.finish()
    }

    // determines which datanode to store the given block id on
    // we calculate this by: block_id % num_datanodes = datanode_id
    fn get_datanode_from_blockid(&self, block_id: u64) -> Option<DataNodeInfo> {
        // first have to get the number of live datanodes by finding length of datanodes: Mutex<HashMap<String, DataNodeInfo>>
        let datanodes = self.datanodes.lock().unwrap();
        let num_datanodes = datanodes.len() as u64;
        if num_datanodes == 0 {
            return None;
        }

        let datanode_id = block_id % num_datanodes;
        datanodes.get(&datanode_id).cloned()
    }

    /// Adds a file to the system, and returns the addresses of the datanodes its chunks live on
    /// The index is the index of the block in the file
    /// i.e. the String at index 0 is the address of the datanode that the first block lives on
    pub async fn add_file(
        &self,
        file_path: &str,
        file_size: usize,
    ) -> Result<Vec<Vec<String>>, Box<dyn Error>> {
        let num_blocks = file_size / DEFAULT_BLOCK_SIZE as usize;
        let mut addrs = Vec::<Vec<String>>::with_capacity(num_blocks);

        for i in 0..num_blocks {
            let block_path = format!("{}_{}", file_path, i);
            let addr = self.add_block(block_path).await?;
            addrs.push(addr);
        }

        Ok(addrs)
    }

    // Adds the new file block to block_records
    pub async fn add_block(&self, block_path: String) -> Result<Vec<String>, Box<dyn Error>> {
        let file_id = Self::get_file_id(&block_path);
        let datanode = match self.get_datanode_from_blockid(file_id) {
            Some(node) => node,
            None => {
                return Err("No Datanodes Running".into());
            }
        };

        let mut block_records = self.block_records.write()
            .map_err(|e| e.to_string())?;
        let datanodes = block_records.add_block_to_records(file_id, datanode.addr)?;
        Ok(datanodes)

        // // get the datanode from file_id and return
        // match self.get_datanode_from_blockid(file_id) {
        //     Some(node) => match block_records.add_block_replicate(&file_id, node.addr.clone()) {
        //         Ok(()) => Ok(node.addr),
        //         Err(_err) => Err("Block Not in Records"),
        //     },
        //     None => {
        //         println!("Datanode not found for file id: {}", file_id);
        //         Err("No Datanodes Running")
        //     }
        // }
    }

    // Removes a file block from block_records, amd returns the datanode addresses it lives on
    pub async fn remove_file(&self, file_path: &str, _owner: i64) -> Result<Vec<String>, &str> {
        let file_id = Self::get_file_id(file_path);
        let mut block_records = self.block_records.write().unwrap();
        block_records.remove_block_from_records(&file_id).ok_or("Block does not exist")
    }

    // returns a vector of datanode addresses that the file lives on
    pub async fn get_file_addresses(
        &self,
        file_path: &str,
        _owner: i64,
    ) -> Result<Vec<String>, &str> {
        let file_id = Self::get_file_id(file_path);
        let block_records = self.block_records.read().unwrap();

        match block_records.get_block_datanodes(&file_id) {
            Ok(datanodes) => Ok(datanodes),
            Err(_) => Err("Block Not in Records"),
        }
    }

    // adds datanode to records
    fn add_datanode(&self, addr: &str) {
        let mut datanodes = self.datanodes.lock().unwrap();
        let mut datanode_ids = self.datanode_ids.lock().unwrap();

        if datanode_ids.get(addr).is_some() {
            return;
        }

        let new_id = self
            .datanode_id_counter
            .fetch_add(1, atomic::Ordering::SeqCst) as u64;
        let info = DataNodeInfo {
            _id: new_id,
            addr: addr.to_string(),
            alive: true,
        };
        datanode_ids.insert(addr.to_string(), new_id);
        datanodes.insert(new_id, info);
    }

    pub async fn record_heartbeat(
        &self,
        address: &str,
        // timestamp:
    ) {
        let mut heartbeats = self.heartbeat_records.lock().unwrap();
        if !heartbeats.contains_key(address) {
            println!("New datanode at address: {}", address);
            // process new datanode by adding it to system
            self.add_datanode(address);
        }
        // update heartbeat time record
        heartbeats.insert(address.to_string(), SystemTime::now());

        println!("Current datanode heartbeats:");
        for (addr, time) in heartbeats.iter() {
            println!("Datanode {}: {:?}", addr, time);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_add_datanode() {
        let records = NameNodeRecords::new();
        let datanode = "127.0.0.1:5000";

        records.add_datanode(&datanode);

        let datanodes = records.datanodes.lock().unwrap();
        let datanode_ids = records.datanode_ids.lock().unwrap();
        assert_eq!(datanodes.len(), 1);
        assert_eq!(datanode_ids.len(), 1);

        let datanode_info = datanodes.get(&0).unwrap();
        assert_eq!(datanode_info.addr, datanode);
    }

    // testing with one datanode in the system
    #[tokio::test]
    async fn test_add_read_remove_file_1() {
        let records = NameNodeRecords::new();
        let datanode = "127.0.0.1:5000";
        records.add_datanode(&datanode);

        let file_path = "test_file";
        let owner_uid = 123;

        // test adding a file returns the correct datanode address
        let result = records.add_file(file_path, owner_uid).await;
        assert!(result.is_ok());
        let datanode_addr = result.unwrap();
        assert_eq!(datanode_addr, datanode);

        // test reading/getting file returns correct datanode address
        let addresses = records.get_file_addresses(file_path, owner_uid).await;
        assert!(addresses.is_ok());
        let addrs = addresses.unwrap();
        assert_eq!(addrs.len(), 1);
        assert_eq!(addrs, vec![datanode]);

        // test removing file returns correct datanode address
        let removal_result: Result<Vec<String>, &str> =
            records.remove_file(file_path, owner_uid).await;
        assert!(removal_result.is_ok());
        let remove_addr = removal_result.unwrap();
        assert_eq!(remove_addr, vec![datanode]);

        // test file is actually removed after removal
        let addresses_after_removal = records.get_file_addresses(file_path, owner_uid).await;
        assert!(addresses_after_removal.is_err());
    }

    // testing with multiple datanodes in the system
    #[tokio::test]
    async fn test_add_read_remove_file_2() {
        let records = NameNodeRecords::new();
        let datanode1 = "127.0.0.1:5000";
        let datanode2 = "127.0.0.1:5001";
        let datanode3 = "127.0.0.1:5002";
        records.add_datanode(&datanode1);
        records.add_datanode(&datanode2);
        records.add_datanode(&datanode3);

        let file_path_0 = "test_file";
        let file_path_1 = "test_file_1";
        let owner_uid = 123;

        // test adding files
        let result = records.add_file(file_path_0, owner_uid).await;
        assert!(result.is_ok());
        let datanode_0 = result.unwrap();
        let result_1 = records.add_file(file_path_1, owner_uid).await;
        assert!(result_1.is_ok());
        let datanode_1 = result_1.unwrap();

        // test reading files
        let read_result = records.get_file_addresses(file_path_0, owner_uid as i64).await;
        assert!(read_result.is_ok());
        assert_eq!(read_result.unwrap(), vec![datanode_0.clone()]);

        let read_result_1 = records.get_file_addresses(file_path_1, owner_uid as i64).await;
        assert!(read_result_1.is_ok());
        assert_eq!(read_result_1.unwrap(), vec![datanode_1.clone()]);
        // println!("{}, {}", datanode_0, datanode_1);

        // testing deletes
        let removal_result: Result<Vec<String>, &str> =
            records.remove_file(file_path_0, owner_uid).await;
        assert!(removal_result.is_ok());
        let remove_addr = removal_result.unwrap();
        assert_eq!(remove_addr, vec![datanode_0]);

        let removal_result: Result<Vec<String>, &str> =
            records.remove_file(file_path_1, owner_uid).await;
        assert!(removal_result.is_ok());
        let remove_addr = removal_result.unwrap();
        assert_eq!(remove_addr, vec![datanode_1]);
    }
}
