#![allow(dead_code, unused_variables, unused_imports)]
use crate::datanode::writer::{self, Writer};
use crate::proto::{client_protocols_client::ClientProtocolsClient, GenericReply};
use crate::proto::{
    client_protocols_server::{ClientProtocols, ClientProtocolsServer},
    ClientInfo, CreateFileRequest, CreateFileResponse, DeleteFileRequest, DeleteFileResponse,
    FileInfo, NodeStatus, ReadFileRequest, ReadFileResponse, SystemInfoRequest, SystemInfoResponse,
    UpdateFileRequest, UpdateFileResponse,
};
use prost_types::compiler::code_generator_response::File as prost_file;
use std::result::Result;
use std::{
    collections::{hash_map::DefaultHasher, HashMap},
    hash::{Hash, Hasher},
    net::SocketAddr,
    str::FromStr,
};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, BufReader};
use tonic::transport::Server;
use tonic::Response;

use crate::datanode::DataNodeServer;

use crate::block::Block;

#[derive(Clone, Hash)]
struct FileMetadata {
    file_id: u64, // Hash(filename || owner)
    file_name: String,
    size: i64,
    owner: i64,
}

#[derive(Clone)]
struct DataNode {
    node_id: i64,
    is_online: bool, // detemined by heartbeat messages
}

#[derive(Clone)]
pub struct NameNodeServer {
    num_datanodes: i64,
    datanodes: Vec<DataNodeServer>,
    blocks: HashMap<String, Vec<DataNodeServer>>,
    metadata: HashMap<u64, FileMetadata>, // from file id : file metadata
    addr: String,
    records: NameNodeRecords,
}

#[derive(Clone)]
pub struct NameNodeRecords {}

impl NameNodeRecords {
    pub fn get_block(&self, file_name: String) -> Block {
        Block::new(file_name, vec![])
    }
}

impl NameNodeServer {
    pub fn new(port: String) -> Self {
        NameNodeServer {
            datanodes: Vec::new(),
            num_datanodes: 0,
            blocks: HashMap::new(),
            metadata: HashMap::new(),
            addr: format!("127.0.0.1:{}", port),
            records: NameNodeRecords {},
        }
    }

    pub async fn run_nameserver(&self) -> Result<(), Box<dyn std::error::Error>> {
        let socket = match SocketAddr::from_str(&self.addr) {
            Ok(socket_addr) => socket_addr,
            Err(err) => {
                eprintln!("Error parsing socket address: {}", err);
                return Err(err.into());
            }
        };
        let client_protocols_service = NameNodeService {
            server: self.clone(),
        };
        println!("Server listening on {}", self.addr);

        Server::builder()
            .add_service(ClientProtocolsServer::new(client_protocols_service))
            .serve(socket)
            .await?;

        Ok(())
    }

    // fn create_file(&mut self, file_id: u64, file_name: &str, file_size: i64, uid: i64) -> bool {
    //     if self.metadata.contains_key(&file_id) {
    //         return false;
    //     }
    //     let file_data = FileMetadata {
    //         file_id: file_id,
    //         file_name: file_name.to_string(),
    //         size: file_size,
    //         owner: uid,
    //     };

    //     self.metadata.insert(file_id, file_data);
    //     true
    // }

    // returns hash(file_name || uid)
    fn get_file_id(file_name: &str, uid: i64) -> u64 {
        let mut hasher = DefaultHasher::new();
        (file_name, uid).hash(&mut hasher);
        hasher.finish()
    }

    fn get_datanode(&self, file_id: String) -> Option<&Vec<DataNodeServer>> {
        self.blocks.get(&file_id)
    }
}

// #[derive(Debug, Default)]
struct NameNodeService {
    server: NameNodeServer,
}

impl NameNodeService {
    fn new(server: NameNodeServer) -> Self {
        Self { server }
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
            node_id: 0,
            is_online: true,
        };
        let datanode_status = self
            .server
            .datanodes
            .iter()
            .map(|datanode| {
                let node_id: i64 = datanode
                    .datanode_addr
                    .to_string()
                    .split(':')
                    .last()
                    .unwrap()
                    .parse::<i64>()
                    .unwrap();
                NodeStatus {
                    node_id,
                    is_online: datanode.is_online,
                }
            })
            .collect();
        let response = SystemInfoResponse {
            namenode: Some(namenode_status),
            nodes: datanode_status,
            num_datanodes: self.server.num_datanodes,
        };

        Ok(Response::new(response))
    }

    async fn create_file(
        &self,
        request: tonic::Request<CreateFileRequest>,
    ) -> Result<tonic::Response<CreateFileResponse>, tonic::Status> {
        let create_request = request.into_inner();

        let FileInfo {
            file_path,
            file_size,
        } = create_request.file_info.unwrap();

        let datanode = self.server.get_datanode(file_path.to_string()).unwrap()[0];

        let mut writer = Writer::new(datanode.datanode_addr, file_path.to_string()).await;
        let mut reader = BufReader::new(tokio::fs::File::open(file_path).await?);
        let mut buffer: Vec<u8> = vec![];

        while let read = reader.read(&mut buffer).await? {
            writer.write(&buffer).await?;
        }

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
    ) -> Result<tonic::Response<UpdateFileResponse>, tonic::Status> {
        unimplemented!()
    }

    async fn delete_file(
        &self,
        request: tonic::Request<DeleteFileRequest>,
    ) -> Result<tonic::Response<DeleteFileResponse>, tonic::Status> {
        let response = DeleteFileResponse {
            response: Some(GenericReply { is_success: true }),
        };
        Ok(Response::new(response))
    }

    async fn read_file(
        &self,
        request: tonic::Request<ReadFileRequest>,
    ) -> Result<tonic::Response<ReadFileResponse>, tonic::Status> {
        let read_request = request.into_inner();
        let file_path = &read_request.file_info.unwrap().file_path;
        let block: Block = self.server.records.get_block(file_path.to_string());

        let response = ReadFileResponse {
            file_content: block.read(),
        };
        Ok(Response::new(response))
    }
}
