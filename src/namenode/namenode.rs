use crate::proto::GenericReply;
#[allow(unused_imports)]
use crate::proto::{
    client_protocols_server::{ClientProtocols, ClientProtocolsServer},
    ClientInfo, CreateFileRequest, CreateFileResponse, DeleteFileRequest, DeleteFileResponse,
    FileInfo, NodeStatus, ReadFileRequest, ReadFileResponse, SystemInfoRequest, SystemInfoResponse,
    UpdateFileRequest, UpdateFileResponse,
};
use std::{net::SocketAddr, str::FromStr};
use tonic::transport::Server;
use tonic::Response;
use crate::namenode::records::{NameNodeRecords, DataNodeInfo};
use std::sync::Arc;

pub struct NameNodeServer {
    // datanodes: Vec<DataNode>,
    // num_datanodes: i64,
    // blocks: HashMap<u64, Vec<DataNode>>,
    // metadata: HashMap<u64, FileMetadata>, // from file id : file metadata
    // addr: String,

    address: String,
    records: Arc<NameNodeRecords>
}

impl NameNodeServer {
    pub fn new(port: String) -> Self {
        Self {
            address: format!("127.0.0.1:{}", port),
            records: Arc::new(NameNodeRecords::new())
        }
    }

    pub async fn run_nameserver(&self) -> Result<(), Box<dyn std::error::Error>> {
        let socket = match SocketAddr::from_str(&self.address) {
            Ok(socket_addr) => socket_addr,
            Err(err) => {
                eprintln!("Error parsing socket address: {}", err);
                return Err(err.into());
            }
        };
        let client_protocols_service = NameNodeService::new(
            self.address.clone(), 
            Arc::clone(&self.records)
        );
        println!("Server listening on {}", self.address);

        Server::builder()
            .add_service(ClientProtocolsServer::new(client_protocols_service))
            .serve(socket)
            .await?;

        Ok(())
    }

}

// #[derive(Debug, Default)]
struct NameNodeService {
    address: String,
    records: Arc<NameNodeRecords>
}

impl NameNodeService {
    fn new(address: String, records: Arc<NameNodeRecords>) -> Self {
        Self { 
            address,
            records
        }
    }
}

#[tonic::async_trait]
impl ClientProtocols for NameNodeService {
    async fn get_system_status(
        &self,
        request: tonic::Request<SystemInfoRequest>,
    ) -> Result<tonic::Response<SystemInfoResponse>, tonic::Status> {
        let system_info_request = request.into_inner();

        if let Some(client_info) = system_info_request.client {
            println!(
                "Received SystemInfoRequest from client: {}",
                client_info.uid
            );
        } else {
            eprintln!("Received SystemInfoRequest with no ClientInfo");
        }

        let namenode_status = NodeStatus {
            node_address: self.address.clone(),
            is_online: true,
        };
        let nodes = self.records.get_datanode_statuses().await;
        let nodes_statuses = nodes.iter().map(|node| NodeStatus {
            node_address: node.addr.clone(),
            is_online: node.alive
        }).collect();

        let response = SystemInfoResponse {
            namenode: Some(namenode_status),
            nodes: nodes_statuses,
            num_datanodes: nodes.len() as i64,
        };

        Ok(Response::new(response))
        // Ok(Response::new(SystemInfoResponse { namenode: None, nodes: vec![], num_datanodes: 0 }))
    }

    async fn create_file(
        &self,
        request: tonic::Request<CreateFileRequest>,
    ) -> Result<tonic::Response<CreateFileResponse>, tonic::Status> {
        let create_request = request.into_inner();

        // if let Some(FileInfo { file_path, file_size }) = create_request.file_info {
        //     if let Some(ClientInfo { uid }) = create_request.client {
        //         let file_id = NameNodeServer::get_file_id(&file_path, uid);
        //         self.server.create_file(file_id, &file_path, file_size, uid);
        //     }
        // }

        let response = CreateFileResponse {
            response: Some(GenericReply { is_success: true }),
        };
        Ok(Response::new(response))
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
