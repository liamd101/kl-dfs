use crate::namenode::records::NameNodeRecords;
use crate::proto::NodeList;
use crate::proto::{
    client_protocols_server::{ClientProtocols, ClientProtocolsServer},
    hearbeat_protocol_server::{HearbeatProtocol, HearbeatProtocolServer},
    CreateFileRequest, CreateFileResponse, DeleteFileRequest, DeleteFileResponse, FileInfo,
    GenericReply, Heartbeat, NodeStatus, ReadFileRequest, ReadFileResponse, SystemInfoRequest,
    SystemInfoResponse, UpdateFileRequest, UpdateFileResponse,
};

use std::net::SocketAddr;
use std::sync::Arc;

use tonic::transport::Server;
use tonic::Response;

pub struct NameNodeServer {
    // datanodes: Vec<DataNode>,
    // num_datanodes: i64,
    // blocks: HashMap<u64, Vec<DataNode>>,
    // metadata: HashMap<u64, FileMetadata>, // from file id : file metadata
    // addr: String,
    address: SocketAddr,
    records: Arc<NameNodeRecords>,
}

impl NameNodeServer {
    pub fn new(port: u16, block_size: usize) -> Self {
        let address = SocketAddr::from(([127, 0, 0, 1], port));
        Self {
            address,
            records: Arc::new(NameNodeRecords::new(block_size)),
        }
    }

    pub async fn run_nameserver(&self) -> Result<(), Box<dyn std::error::Error>> {
        let client_protocols_service =
            NameNodeService::new(self.address.to_string(), Arc::clone(&self.records));
        println!("Server listening on {}", self.address);

        Server::builder()
            .add_service(ClientProtocolsServer::new(client_protocols_service))
            .add_service(HearbeatProtocolServer::new(HeartbeatRecordService::new(
                Arc::clone(&self.records),
            )))
            .serve(self.address)
            .await?;

        Ok(())
    }
}

// #[derive(Debug, Default)]
struct NameNodeService {
    address: String,
    records: Arc<NameNodeRecords>,
}

impl NameNodeService {
    fn new(address: String, records: Arc<NameNodeRecords>) -> Self {
        Self { address, records }
    }
}

impl From<Vec<String>> for NodeList {
    fn from(val: Vec<String>) -> Self {
        NodeList {
            nodes: val.into_iter().collect(),
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
        let nodes_statuses = nodes
            .iter()
            .map(|node| NodeStatus {
                node_address: node.addr.clone(),
                is_online: node.alive,
            })
            .collect();

        let response = SystemInfoResponse {
            namenode: Some(namenode_status),
            nodes: nodes_statuses,
            num_datanodes: nodes.len() as i64,
        };

        Ok(Response::new(response))
    }

    async fn create_file(
        &self,
        request: tonic::Request<CreateFileRequest>,
    ) -> Result<tonic::Response<CreateFileResponse>, tonic::Status> {
        println!("Received CreateFileRequest");
        let create_request = request.into_inner();
        let FileInfo {
            file_path,
            file_size,
        } = create_request
            .file_info
            .expect("File information not provided");

        let addresses = match self.records.add_file(&file_path, file_size as usize).await {
            Ok(addresses) => addresses,
            Err(err) => {
                println!("{}", err);
                return Err(tonic::Status::internal(
                    "Failed to add file (no datanodes running)",
                ));
            }
        };

        println!("DataNode addresses: {:?}", addresses);

        let response = CreateFileResponse {
            datanode_addrs: addresses.into_iter().map(|addr| addr.into()).collect(),
            response: Some(GenericReply {
                is_success: true,
                message: format!("Create request successfully processed for: {}", file_path),
            }),
        };
        Ok(Response::new(response))
    }

    // receives an update file request, returns a list of namenode addresses containing that file
    async fn update_file(
        &self,
        request: tonic::Request<UpdateFileRequest>,
    ) -> std::result::Result<tonic::Response<UpdateFileResponse>, tonic::Status> {
        println!("Received UpdateFileRequest");
        let update_request = request.into_inner();

        let FileInfo {
            file_path,
            file_size,
        } = update_request
            .file_info
            .expect("File information not provided");

        let addresses = match self
            .records
            .update_file(&file_path, file_size as usize)
            .await
        {
            Ok(addresses) => addresses,
            Err(err) => {
                println!("{}", err);
                return Err(tonic::Status::internal("File does not exist"));
            }
        };

        let upd_response = UpdateFileResponse {
            datanode_addrs: addresses.into_iter().map(|addr| addr.into()).collect(),
            response: Some(GenericReply {
                is_success: true,
                message: format!("Update request successfully processed for: {}", file_path),
            }),
        };
        Ok(Response::new(upd_response))
    }

    async fn delete_file(
        &self,
        request: tonic::Request<DeleteFileRequest>,
    ) -> std::result::Result<tonic::Response<DeleteFileResponse>, tonic::Status> {
        println!("Received DeleteFileRequest");
        let delete_request = request.into_inner();

        let FileInfo {
            file_path,
            file_size: _,
        } = delete_request
            .file_info
            .expect("File information not provided");

        let addresses = match self.records.remove_file(&file_path).await {
            Ok(addresses) => addresses,
            Err(err) => {
                println!("{}", err);
                return Err(tonic::Status::internal("File does not exist"));
            }
        };

        println!("DataNode addresses: {:?}", addresses);

        let del_response = DeleteFileResponse {
            datanode_addrs: addresses.into_iter().map(|addr| addr.into()).collect(),
            response: Some(GenericReply {
                is_success: true,
                message: format!("Delete request succesfully processed for: {}", file_path),
            }),
        };
        Ok(Response::new(del_response))
    }

    // returns list of datanode addresses containing this file
    async fn read_file(
        &self,
        request: tonic::Request<ReadFileRequest>,
    ) -> std::result::Result<tonic::Response<ReadFileResponse>, tonic::Status> {
        println!("Received ReadFileRequest");
        let read_request = request.into_inner();

        let FileInfo {
            file_path,
            file_size: _,
        } = read_request
            .file_info
            .expect("File information not provided");

        let datanode_addr = match self.records.get_file_addresses(&file_path).await {
            Ok(addresses) => addresses,
            Err(err) => {
                println!("{}", err);
                return Err(tonic::Status::internal("File does not exist"));
            }
        };

        let reply = GenericReply {
            is_success: true,
            message: format!("Read request successfully processed for: {}", file_path),
        };
        let read_resp = ReadFileResponse {
            response: Some(reply), // why does this have to be an option?
            datanode_addrs: datanode_addr.into_iter().map(|addr| addr.into()).collect(),
        };
        Ok(Response::new(read_resp))
    }
}

struct HeartbeatRecordService {
    records: Arc<NameNodeRecords>,
}

impl HeartbeatRecordService {
    fn new(records: Arc<NameNodeRecords>) -> Self {
        Self { records }
    }
}

#[tonic::async_trait]
impl HearbeatProtocol for HeartbeatRecordService {
    async fn send_heartbeat(
        &self,
        request: tonic::Request<Heartbeat>,
    ) -> std::result::Result<tonic::Response<GenericReply>, tonic::Status> {
        let incoming_heartbeat = request.into_inner();

        let Heartbeat { address } = incoming_heartbeat;

        self.records.record_heartbeat(&address).await;
        let reply = GenericReply {
            is_success: true,
            message: "Heartbeat recorded successfully".to_string(),
        };

        Ok(Response::new(reply))
    }
}
