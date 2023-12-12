use crate::namenode::block_records::BlockRecords;
use std::collections::HashMap;
// for atomic counter for id generation
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::SeedableRng;
use std::collections::hash_map::DefaultHasher;
use std::error::Error;
use std::hash::{Hash, Hasher};
use std::sync::atomic::AtomicUsize;
use std::sync::{atomic, Mutex, RwLock};
use std::time::SystemTime;

#[derive(Clone)]
pub struct DataNodeInfo {
    _id: u64,
    pub addr: String,
    pub alive: bool,
}

/// recordkeeper/bookkeeper for namenode information
pub struct NameNodeRecords {
    /// max block size in bytes
    block_size: usize,

    /// maps datanode id to datanode info
    datanodes: Mutex<HashMap<u64, DataNodeInfo>>, // datanode id : datanode info

    /// maps datanode ip address to datanode id
    datanode_ids: Mutex<HashMap<String, u64>>,

    /// maps blocks to block metadata (including which datanodes a block is on)
    block_records: RwLock<BlockRecords>,

    datanode_id_counter: AtomicUsize,
    /// map from datanode ip address to time of last message
    heartbeat_records: Mutex<HashMap<String, SystemTime>>,

    /// map from file path to block ids
    file_records: Mutex<HashMap<String, usize>>,

    /// Number of replicas to store for each block
    replication_count: usize,
}

impl Default for NameNodeRecords {
    fn default() -> Self {
        Self::new(3, 4096)
    }
}

impl NameNodeRecords {
    pub fn new(replication_count: usize, block_size: usize) -> Self {
        Self {
            block_size,
            datanodes: Mutex::new(HashMap::new()),
            datanode_ids: Mutex::new(HashMap::new()),
            block_records: RwLock::new(BlockRecords::new()),
            datanode_id_counter: AtomicUsize::new(0),
            heartbeat_records: Mutex::new(HashMap::new()),
            file_records: Mutex::new(HashMap::new()),
            replication_count,
        }
    }

    pub async fn get_datanode_statuses(&self) -> Vec<DataNodeInfo> {
        let datanodes = self.datanodes.lock().unwrap();
        let statuses = datanodes.values().cloned().collect();
        statuses
    }

    /// returns hash(file_name || uid)
    fn get_file_id(file_name: &str) -> u64 {
        // get file_id from hashing
        let mut hasher = DefaultHasher::new();
        file_name.hash(&mut hasher);
        hasher.finish()
    }

    /// Adds a file to the system, and returns the addresses of the datanodes its chunks live on
    /// The index is the index of the block in the file
    /// i.e. the String at index 0 is a list of addresses that the first block lives on
    pub async fn add_file(
        &self,
        file_path: &str,
        file_size: usize,
    ) -> Result<Vec<Vec<String>>, Box<dyn Error>> {
        let num_blocks = (file_size + (self.block_size - 1)) / self.block_size;
        let mut addrs = Vec::<Vec<String>>::with_capacity(num_blocks);

        for i in 0..num_blocks {
            let block_path = format!("{}_{}", file_path, i);
            let addr = self.add_block(block_path).await?;
            addrs.push(addr);
        }

        let mut file_records = self.file_records.lock().unwrap();
        file_records.insert(file_path.to_string(), num_blocks);
        drop(file_records);

        Ok(addrs)
    }

    /// Adds a new block to block_records and returns a list of datanode addresses to store the block on
    async fn add_block(&self, block_path: String) -> Result<Vec<String>, Box<dyn Error>> {
        let file_id = Self::get_file_id(&block_path);
        let datanodes = self.get_datanode_statuses().await;

        // randomly select 3 datanodes to store the block on
        let mut rng = StdRng::seed_from_u64(file_id);
        let mut shuffled_datanodes = datanodes.clone();
        shuffled_datanodes.shuffle(&mut rng);
        let selected_datanodes = shuffled_datanodes
            .into_iter()
            .take(self.replication_count)
            .map(|datanode| datanode.addr)
            .collect();

        let mut block_records = self.block_records.write().map_err(|e| e.to_string())?;
        let datanodes = block_records.add_block_to_records(file_id, selected_datanodes)?;
        Ok(datanodes)
    }

    pub async fn update_file(
        &self,
        file_path: &str,
        file_size: usize,
    ) -> Result<Vec<Vec<String>>, Box<dyn Error>> {
        let num_blocks = (file_size + (self.block_size - 1)) / self.block_size;
        let mut addrs = Vec::<Vec<String>>::with_capacity(num_blocks);

        for i in 0..num_blocks {
            let block_path = format!("{}_{}", file_path, i);
            let addr = self.add_block(block_path).await?;
            addrs.push(addr);
        }

        let mut file_records = self.file_records.lock().unwrap();
        let prev_block_count = file_records
            .insert(file_path.to_string(), num_blocks)
            .unwrap_or(0usize);
        drop(file_records);

        println!("Previous block count: {}", prev_block_count);
        println!("Current block count: {}", num_blocks);

        for i in num_blocks..prev_block_count {
            let block_path = format!("{}_{}", file_path, i);
            let addr = self
                .remove_block(&block_path)
                .expect("Block does not exist");
            addrs.push(addr);
        }

        Ok(addrs)
    }

    pub async fn remove_file(&self, file_path: &str) -> Result<Vec<Vec<String>>, Box<dyn Error>> {
        let mut file_records = self.file_records.lock().unwrap();
        let num_blocks = file_records.remove(file_path).unwrap_or(0usize);

        let mut addrs = Vec::<Vec<String>>::with_capacity(num_blocks);

        for i in 0..num_blocks {
            let block_path = format!("{}_{}", file_path, i);
            let addr = self
                .remove_block(&block_path)
                .expect("Block does not exist");
            addrs.push(addr);
        }

        Ok(addrs)
    }

    // Removes a file block from block_records, amd returns the datanode addresses it lives on
    fn remove_block(&self, file_path: &str) -> Result<Vec<String>, &str> {
        let file_id = Self::get_file_id(file_path);
        let mut block_records = self.block_records.write().unwrap();
        block_records
            .remove_block_from_records(&file_id)
            .ok_or("Block does not exist")
    }

    /// Returns a 2d vector of datanode addresses that each block of the file lives on
    pub async fn get_file_addresses(
        &self,
        file_path: &str,
    ) -> Result<Vec<Vec<String>>, Box<dyn Error>> {
        let file_records = self.file_records.lock().unwrap();
        let num_blocks = file_records.get(file_path).copied().unwrap_or(0usize);
        let mut addrs = Vec::<Vec<String>>::with_capacity(num_blocks);

        for i in 0..num_blocks {
            let block_path = format!("{}_{}", file_path, i);
            let addr = self
                .get_block_addresses(block_path)
                .expect("Block does not exist");
            addrs.push(addr);
        }

        Ok(addrs)
    }

    /// Returns a vector of datanode addresses that the file lives on
    fn get_block_addresses(&self, file_path: String) -> Result<Vec<String>, &str> {
        let file_id = Self::get_file_id(&file_path);
        let block_records = self.block_records.read().unwrap();

        match block_records.get_block_datanodes(&file_id) {
            Ok(datanodes) => Ok(datanodes),
            Err(_) => Err("Block Not in Records"),
        }
    }

    /// Adds datanode to records
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

    pub async fn record_heartbeat(&self, address: &str) {
        let mut heartbeats = self.heartbeat_records.lock().unwrap();
        if !heartbeats.contains_key(address) {
            println!("New datanode at address: {}", address);
            // process new datanode by adding it to system
            self.add_datanode(address);
        }
        // update heartbeat time record
        heartbeats.insert(address.to_string(), SystemTime::now());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_add_datanode() {
        let records = NameNodeRecords::new(1, 4096);
        let datanode = "127.0.0.1:5000";

        records.add_datanode(&datanode);

        let datanodes = records.datanodes.lock().unwrap();
        let datanode_ids = records.datanode_ids.lock().unwrap();
        assert_eq!(datanodes.len(), 1);
        assert_eq!(datanode_ids.len(), 1);

        let datanode_info = datanodes.get(&0).unwrap();
        assert_eq!(datanode_info.addr, datanode);
    }

    // testing with one datanode in the system, replication of 1
    #[tokio::test]
    async fn test_add_read_remove_file_1() {
        let records = NameNodeRecords::new(1, 4096);
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

    // testing with multiple datanodes in the system, replication of 1
    #[tokio::test]
    async fn test_add_read_remove_file_2() {
        let records = NameNodeRecords::new(1, 4096);
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
        let read_result = records
            .get_file_addresses(file_path_0, owner_uid as i64)
            .await;
        assert!(read_result.is_ok());
        assert_eq!(read_result.unwrap(), vec![datanode_0.clone()]);

        let read_result_1 = records
            .get_file_addresses(file_path_1, owner_uid as i64)
            .await;
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

    #[tokio::test]
    async fn test_replication() {
        let records = NameNodeRecords::new(2, 4096);
        let datanode1 = "127.0.0.1:5000";
        let datanode2 = "127.0.0.1:5001";
        let datanode3 = "127.0.0.1:5002";
        records.add_datanode(&datanode1);

        // testing replication when replication factor > number of datanodes
        let file_path = "test_file";
        let owner_uid = 123;
        let datanode_ips = records.add_file_replicas(file_path, owner_uid).await;
        assert!(datanode_ips.is_ok());
        assert_eq!(datanode_ips.unwrap(), vec![datanode1.clone()]);

        // testing replication when replication factor = number of datanodes
        let file_path_2 = "test_file_2";
        records.add_datanode(&datanode2);
        let datanode_ips = records.add_file_replicas(file_path_2, owner_uid).await;
        assert!(datanode_ips.is_ok());
        let d_ips = datanode_ips.unwrap();
        assert_eq!(d_ips.len(), 2);
        assert!(d_ips.contains(&datanode1.to_string()));
        assert!(d_ips.contains(&datanode2.to_string()));

        // testing replication when replication factor < number of datanodes
        let file_path_3 = "test_file_3";
        records.add_datanode(&datanode3);
        let datanode_ips = records.add_file_replicas(file_path_3, owner_uid).await;
        assert!(datanode_ips.is_ok());
        let d_ips = datanode_ips.unwrap();
        assert_eq!(d_ips.len(), 2);
        // println!("{:?}", d_ips);
    }
}
