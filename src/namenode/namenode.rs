
use std::collections::HashMap;
#[allow(unused_imports)]
use tonic::transport::Server;
// use network_comms::client_protocols_server::{ClientProtocols, ClientProtocolsServer};
// use network_comms::{
//     system_info_request::SystemInfoRequest,
//     system_info_response::SystemInfoResponse,
//     create_file_request::CreateFileRequest,
//     create_file_response::CreateFileResponse,
//     update_file_request::UpdateFileRequest,
//     update_file_response::UpdateFileResponse,
//     delete_file_request::DeleteFileRequest,
//     delete_file_response::DeleteFileResponse,
//     read_file_request::ReadFileRequest,
//     read_file_response::ReadFileResponse,
//     client_info::ClientInfo,
//     node_status::NodeStatus,
//     file_info::FileInfo,
//     generic_reply::GenericReply,
// };
#[allow(unused_imports)]
use crate::proto::{
    client_protocols_server::{ClientProtocols, ClientProtocolsServer},
    SystemInfoRequest, SystemInfoResponse,
    CreateFileRequest, CreateFileResponse,
    UpdateFileRequest, UpdateFileResponse,
    DeleteFileRequest, DeleteFileResponse,
    ReadFileRequest, ReadFileResponse,
    ClientInfo
};

struct FileMetadata {
    file_id: u64, // Hash(filename || owner)
    file_name: String,
    size: u64,
    owner: i64,
}

struct DataNode;

pub struct NameNodeServer {
    datanodes: Vec<DataNode>,
    num_datanodes: i64,
    blocks: HashMap<u64, Vec<DataNode>>,
    metadata: HashMap<u64, FileMetadata>, // from file id : file metadata
    addr: String,
}

impl NameNodeServer {
    pub fn new(port: String) -> Self {
        NameNodeServer {
            datanodes: Vec::new(),
            num_datanodes: 0,
            blocks: HashMap::new(),
            metadata: HashMap::new(),
            addr: format!("127.0.0.1:{}", port),
        }
    }

    pub async fn run_nameserver(&self) -> Result<SystemInfoResponse, tonic::Status> {
        // let addr = match self.addr.clone().parse() {
        //     Ok(parsed_addr) => {

        //     }
        //     Err(err) => {
        //         eprintln!("Error parsing IP address: {}", err);
        //         None
        //     }
        // };

        let client_protocols_service = ClientProtocolsService::default();
        
        println!("Server listening on {}", self.addr);

        // Server::builder()
        //     .add_service(ClientProtocolsServer::new(client_protocols_service))
        //     .serve(addr)
        //     .await?;

        // Ok(())
        unimplemented!()
    }
}

#[derive(Debug, Default)]
struct ClientProtocolsService;

#[tonic::async_trait]
impl ClientProtocols for ClientProtocolsService {
    async fn get_system_status(
        &self,
        request: tonic::Request<SystemInfoRequest>,
    ) -> Result<tonic::Response<SystemInfoResponse>, tonic::Status> {
        
        unimplemented!()
    }

    async fn create_file(
        &self,
        request: tonic::Request<CreateFileRequest>,
    ) -> Result<tonic::Response<CreateFileResponse>, tonic::Status> {
        // Implement your logic here
        unimplemented!()
    }

    async fn update_file(
        &self,
        request: tonic::Request<UpdateFileRequest>,
    ) -> std::result::Result<tonic::Response<UpdateFileResponse>, tonic::Status> {
        unimplemented!()
    }

    async fn delete_file(
        &self,
        request: tonic::Request<DeleteFileRequest>,
    ) -> std::result::Result<tonic::Response<DeleteFileResponse>, tonic::Status> {
        unimplemented!()
    }

    async fn read_file(
        &self,
        request: tonic::Request<ReadFileRequest>,
    ) -> std::result::Result<tonic::Response<ReadFileResponse>, tonic::Status> {
        unimplemented!()
    }

}