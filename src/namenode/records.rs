use crate::namenode::block_records::BlockRecords;
use std::collections::HashMap;
use std::sync::{atomic, Mutex, RwLock};
// for atomic counter for id generation
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::atomic::AtomicUsize;
use std::time::SystemTime;

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
    replication_count: u64,
}

impl Default for NameNodeRecords {
    fn default() -> Self {
        Self::new(3)
    }
}

// TODO: heartbeat monitor - sends and checks for heartbeats and keeps datanodes updated with alive statuses
// how does it handle if we started making a file, but it wasn't actually written??
impl NameNodeRecords {
    pub fn new(replication_count: u64) -> Self {
        Self {
            datanodes: Mutex::new(HashMap::new()),
            datanode_ids: Mutex::new(HashMap::new()),
            block_records: RwLock::new(BlockRecords::new()),
            datanode_id_counter: AtomicUsize::new(0),
            // default_block_size: 4096,
            heartbeat_records: Mutex::new(HashMap::new()),
            replication_count,
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
    fn calculate_datanode_from_blockid(&self, block_id: u64) -> Option<DataNodeInfo> {
        // first have to get the number of live datanodes by finding length of datanodes: Mutex<HashMap<String, DataNodeInfo>>
        let datanodes = self.datanodes.lock().unwrap();
        let num_datanodes = datanodes.len() as u64;
        if num_datanodes == 0 {
            return None;
        }

        let datanode_id = block_id % num_datanodes;

        datanodes.get(&datanode_id).cloned()
    }

    // we calculate this by: block_id % num_datanodes = datanode_id. Increment to get datanodes to replicate to
    fn calculate_replicas_from_blockid(&self, block_id: u64) -> Option<Vec<DataNodeInfo>> {
        // first have to get the number of live datanodes by finding length of datanodes: Mutex<HashMap<String, DataNodeInfo>>
        let datanodes = self.datanodes.lock().unwrap();
        let num_datanodes = datanodes.len() as u64;
        if num_datanodes == 0 {
            return None;
        }

        let num_replicas = std::cmp::min(self.replication_count, num_datanodes) as usize;
        let mut return_datanodes: Vec<DataNodeInfo> = Vec::with_capacity(num_replicas);

        let starting_datanode_id: u64 = block_id % num_datanodes;
        return_datanodes.push(datanodes.get(&starting_datanode_id).cloned().unwrap());

        for i in 1..num_replicas {
            let datanode_id = (starting_datanode_id + i as u64) % num_datanodes;
            return_datanodes.push(datanodes.get(&datanode_id).cloned().unwrap());
        }

        Some(return_datanodes.clone())
    }

    // Adds the new file block to block_records
    pub async fn add_file(&self, file_path: &str, _owner: i64) -> Result<String, &str> {
        let file_id = Self::get_file_id(file_path);
        let mut block_records = self.block_records.write().unwrap();
        match block_records.add_block_to_records(file_id) {
            Ok(()) => {
                // get the datanode from file_id and return
                match self.calculate_datanode_from_blockid(file_id) {
                    Some(node) => {
                        match block_records.add_block_replicate(&file_id, node.addr.clone()) {
                            Ok(()) => Ok(node.addr),
                            Err(_err) => Err("Block Not in Records"),
                        }
                    }
                    None => {
                        println!("Datanode not found for file id: {}", file_id);
                        Err("No Datanodes Running")
                    }
                }
            }
            Err(_err) => Err("Block Already exists"),
        }
    }

    // Adds the new file block to block_records, with replicas
    pub async fn add_file_replicas(
        &self,
        file_path: &str,
        _owner: i64,
    ) -> Result<Vec<String>, &str> {
        let file_id = Self::get_file_id(file_path);
        let mut block_records = self.block_records.write().unwrap();
        match block_records.add_block_to_records(file_id) {
            Ok(()) => {
                // initialized record for this block in block_records (initialized hashset for this block)
                match self.calculate_replicas_from_blockid(file_id) {
                    Some(nodes) => {
                        let addr_values: Vec<String> =
                            nodes.iter().map(|node| node.addr.clone()).collect();
                        for node in nodes {
                            if block_records
                                .add_block_replicate(&file_id, node.addr.clone())
                                .is_err()
                            {
                                return Err("Block Not in Records (uninitialized when trying to add replica)");
                            }
                        }
                        Ok(addr_values)
                    }
                    None => {
                        println!("Datanode not found for file id: {}", file_id);
                        Err("No Datanodes Running")
                    }
                }
            }
            Err(_err) => Err("Block Already exists"),
        }
    }

    // Removes a file block from block_records, amd returns the datanode addresses it lives on
    pub async fn remove_file(&self, file_path: &str, _owner: i64) -> Result<Vec<String>, &str> {
        let file_id = Self::get_file_id(file_path);
        let mut block_records = self.block_records.write().unwrap();
        match block_records.remove_block_from_records(&file_id) {
            Ok(ip_addresses) => Ok(ip_addresses),
            Err(_err) => Err("Block Already exists"),
        }
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
            Err(_err) => Err("Block Not in Records"),
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

    pub async fn record_heartbeat(&self, address: &str) {
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
        let records = NameNodeRecords::new(1);
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
        let records = NameNodeRecords::new(1);
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
        let records = NameNodeRecords::new(1);
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
        let read_result = records.get_file_addresses(file_path_0, owner_uid).await;
        assert!(read_result.is_ok());
        assert_eq!(read_result.unwrap(), vec![datanode_0.clone()]);

        let read_result_1 = records.get_file_addresses(file_path_1, owner_uid).await;
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
        let records = NameNodeRecords::new(2);
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
