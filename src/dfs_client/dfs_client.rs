use crate::protocols; // bring proto folder into scope

pub struct Client<'a> {
    user_id: i64,
    namenode_address: &'a str, // rpc address: IP & port as a string
}

impl Client<'a> {
    pub fn new(user_id: i64, namenode_address: &str) -> Client {
        Client {
            user_id,
            namenode_address
        }
    }

    // establish a connection with the namenode server stored in self.namenode_address
    // feel free to change the function definition
    // async fn get_connection(&self) -> Result<> {
        
    // }

    // sends a getsysteminfo request to the namenode in self.namenode_address
    pub async fn get_system_info(&self) -> Result<Vec<proto::DataNodeInfo>> {
        
    }


    pub async fn ls(&self, path: impl Into<String>) -> Result<Vec<String>> {
        
    }

    // send a write file request to namenode server
    pub async fn upload(&self, src: &str, dst: impl Into<String>) -> Result<()> {
        
    }

    // send a read file request to namenode server
    pub async fn download(&self, src: &str, dst: &str) -> Result<()> {
        
    }

}