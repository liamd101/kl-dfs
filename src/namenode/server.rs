use crate::namenode::records::NameNodeRecords;
use crate::proto::{
    client_protocols_server::{ClientProtocols, ClientProtocolsServer},
    ClientInfo, CreateFileRequest, CreateFileResponse, DeleteFileRequest, DeleteFileResponse,
    FileInfo, GenericReply, NodeStatus, ReadFileRequest, ReadFileResponse, SystemInfoRequest,
    SystemInfoResponse, UpdateFileRequest, UpdateFileResponse,
};
use crate::proto::{
    hearbeat_protocol_server::{HearbeatProtocol, HearbeatProtocolServer},
    Heartbeat,
};
use std::sync::Arc;
use std::{net::SocketAddr, str::FromStr};
use tonic::transport::Server;
use tonic::Response;

pub struct NameNodeServer {
    // datanodes: Vec<DataNode>,
    // num_datanodes: i64,
    // blocks: HashMap<u64, Vec<DataNode>>,
    // metadata: HashMap<u64, FileMetadata>, // from file id : file metadata
    // addr: String,
    address: String,
    records: Arc<NameNodeRecords>,
}

impl NameNodeServer {
    pub fn new(port: u16) -> Self {
        Self {
            address: format!("127.0.0.1:{}", port),
            records: Arc::new(NameNodeRecords::new()),
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

        let client_protocols_service =
            NameNodeService::new(self.address.clone(), Arc::clone(&self.records));
        println!("Server listening on {}", self.address);

        Server::builder()
            .add_service(ClientProtocolsServer::new(client_protocols_service))
            .add_service(HearbeatProtocolServer::new(HeartbeatRecordService::new(
                Arc::clone(&self.records),
            )))
            .serve(socket)
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
        // Ok(Response::new(SystemInfoResponse { namenode: None, nodes: vec![], num_datanodes: 0 }))
    }

    async fn create_file(
        &self,
        request: tonic::Request<CreateFileRequest>,
    ) -> Result<tonic::Response<CreateFileResponse>, tonic::Status> {
        println!("Received CreateFileRequest");
        let create_request = request.into_inner();

        let FileInfo {
            file_path,
            file_size: _,
        } = create_request
            .file_info
            .expect("File information not provided");
        let ClientInfo { uid } = create_request
            .client
            .expect("Client information not provided");

        let datanode_addr = match self.records.add_file(&file_path, uid).await {
            Ok(address) => address,
            Err(err) => {
                println!("{}", err);
                return Err(tonic::Status::internal(
                    "Failed to add file (no datanodes running)",
                ));
            }
        };

        let response = CreateFileResponse {
            datanode_addr,
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
            file_size: _,
        } = update_request
            .file_info
            .expect("File information not provided");
        let ClientInfo { uid } = update_request
            .client
            .expect("Client information not provided");

        match self.records.get_file_addresses(&file_path, uid).await {
            Ok(addresses) => {
                let upd_response = UpdateFileResponse {
                    response: Some(GenericReply {
                        is_success: true,
                        message: format!(
                            "Update request successfully processed for: {}",
                            file_path
                        ),
                    }),
                    datanode_addr: addresses,
                };
                Ok(Response::new(upd_response))
            }
            Err(err) => {
                println!("{}", err);
                Err(tonic::Status::internal("File does not exist"))
            }
        }
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
        let ClientInfo { uid } = delete_request
            .client
            .expect("Client information not provided");

        match self.records.remove_file(&file_path, uid).await {
            Ok(addresses) => {
                let del_response = DeleteFileResponse {
                    response: Some(GenericReply {
                        is_success: true,
                        message: format!("Delete request succesfully processed for: {}", file_path),
                    }),
                    datanode_addr: addresses,
                };
                Ok(Response::new(del_response))
            }
            Err(err) => {
                println!("{}", err);
                Err(tonic::Status::internal(
                    "Failed to add file (no datanodes running)",
                ))
            }
        }
    }

    // returns list of datanode addresses containing this file
    async fn read_file(
        &self,
        request: tonic::Request<ReadFileRequest>,
    ) -> std::result::Result<tonic::Response<ReadFileResponse>, tonic::Status> {
        println!("Received ReadFileRequest");
        let read_request = request.into_inner();

        if let Some(FileInfo {
            file_path,
            file_size: _,
        }) = read_request.file_info
        {
            if let Some(ClientInfo { uid }) = read_request.client {
                match self.records.get_file_addresses(&file_path, uid).await {
                    Ok(addresses) => {
                        let read_resp = ReadFileResponse {
                            response: Some(GenericReply {
                                is_success: true,
                                message: format!(
                                    "Read request successfully processed for: {}",
                                    file_path
                                ),
                            }),
                            datanode_addr: addresses,
                        };
                        Ok(Response::new(read_resp))
                    }
                    Err(err) => {
                        println!("{}", err);
                        Err(tonic::Status::internal("File does not exist"))
                    }
                }
            } else {
                Err(tonic::Status::internal("Client information not provided"))
            }
        } else {
            Err(tonic::Status::internal("File information not provided"))
        }
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
        println!("Received heartbeat: {:?}", incoming_heartbeat);

        // let Heartbeat{address, time} = incoming_heartbeat;
        let Heartbeat { address } = incoming_heartbeat;

        // self.records.record_heartbeat(&address, time);
        self.records.record_heartbeat(&address).await;
        let reply = GenericReply {
            is_success: true,
            message: "Heartbeat recorded successfully".to_string(),
        };

        Ok(Response::new(reply))
    }
}