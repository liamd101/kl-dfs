use std::error::Error;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::interval;

use crate::proto::data_node_protocols_server::{DataNodeProtocols, DataNodeProtocolsServer};
use crate::proto::{
    hearbeat_protocol_client::HearbeatProtocolClient, CreateBlockRequest, CreateBlockResponse,
    DeleteBlockResponse, DeleteFileRequest, FileInfo, Heartbeat, ReadBlockResponse,
    ReadFileRequest, UpdateBlockResponse, UpdateFileRequest,
};

use crate::datanode::storage::Storage;
use tonic::transport::Channel;
use tonic::transport::Server;

/// Server that runs a datanode
#[derive(Clone)]
pub struct DataNodeServer {
    /// TCP address of the datanode
    pub datanode_addr: SocketAddr,

    /// Block storage of the datanode
    pub storage: Arc<Mutex<Storage>>,

    /// Connection to the namenode
    pub namenode_addr: SocketAddr,
}

impl DataNodeServer {
    pub fn new(port: u16, namenode_port: u16) -> Self {
        let datanode_addr: SocketAddr =
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port);
        let namenode_addr: SocketAddr =
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), namenode_port);
        DataNodeServer {
            datanode_addr,
            storage: Arc::new(Mutex::new(Storage::new())),
            namenode_addr,
        }
    }

    pub async fn run_dataserver(&self) -> Result<(), Box<dyn Error>> {
        let heartbeat_status = self.send_heartbeat_loop();
        let service_status = self.run_service();

        tokio::select! {
            result = heartbeat_status => {
                match result {
                    Ok(_) => {}, // If Ok, do nothing
                    Err(e) => return Err(e), // If Err, return the error
                }
            }
            result = service_status => {
                match result {
                    Ok(_) => {}, // If Ok, do nothing
                    Err(e) => return Err(e), // If Err, return the error
                }
            }
        }

        Ok(())
    }

    pub async fn run_service(&self) -> Result<(), Box<dyn Error>> {
        Server::builder()
            .add_service(DataNodeProtocolsServer::new(self.clone()))
            .serve(self.datanode_addr)
            .await?;
        Ok(())
    }

    pub async fn send_heartbeat_loop(&self) -> Result<(), Box<dyn Error>> {
        let mut interval = interval(Duration::from_secs(5));
        let channel = Channel::from_shared(format!("http://{}", self.namenode_addr))
            .unwrap()
            .connect()
            .await?;

        let mut heartbeat_client = HearbeatProtocolClient::new(channel);

        loop {
            interval.tick().await;
            let request = tonic::Request::new(Heartbeat {
                address: self.datanode_addr.to_string(),
            });
            let response = heartbeat_client.send_heartbeat(request).await?;
            println!("Received heartbeat response: {:?}", response);
        }
    }
}

#[tonic::async_trait]
impl DataNodeProtocols for DataNodeServer {
    async fn create_file(
        &self,
        request: tonic::Request<CreateBlockRequest>,
    ) -> Result<tonic::Response<CreateBlockResponse>, tonic::Status> {
        let request = request.into_inner();
        let block_info = request.block_info.ok_or_else(|| {
            tonic::Status::new(tonic::Code::InvalidArgument, "Block_info not found")
        })?;

        let file_path = request.file_name;

        let mut storage = self.storage.lock().await;
        storage
            .create(&file_path, block_info)
            .await
            .map_err(|_| tonic::Status::new(tonic::Code::Internal, "Failed to create file"))?;
        drop(storage);

        let reply = CreateBlockResponse { success: true };
        Ok(tonic::Response::new(reply))
    }

    async fn update_file(
        &self,
        request: tonic::Request<UpdateFileRequest>,
    ) -> Result<tonic::Response<UpdateBlockResponse>, tonic::Status> {
        let request = request.into_inner();
        let FileInfo {
            file_path: _,
            file_size: _,
        } = request.file_info.ok_or_else(|| {
            tonic::Status::new(tonic::Code::InvalidArgument, "File info not found")
        })?;

        let reply = UpdateBlockResponse { success: true };
        Ok(tonic::Response::new(reply))
    }

    async fn delete_file(
        &self,
        request: tonic::Request<DeleteFileRequest>,
    ) -> Result<tonic::Response<DeleteBlockResponse>, tonic::Status> {
        let request = request.into_inner();
        let FileInfo {
            file_path,
            file_size: _,
        } = request.file_info.ok_or_else(|| {
            tonic::Status::new(tonic::Code::InvalidArgument, "File info not found")
        })?;

        let mut storage = self.storage.lock().await;
        storage
            .delete(&file_path)
            .await
            .expect("Failed to delete file");
        drop(storage);

        let reply = DeleteBlockResponse { success: true };
        Ok(tonic::Response::new(reply))
    }

    async fn read_file(
        &self,
        request: tonic::Request<ReadFileRequest>,
    ) -> Result<tonic::Response<ReadBlockResponse>, tonic::Status> {
        let request = request.into_inner();
        let FileInfo {
            file_path,
            file_size: _,
        } = request.file_info.ok_or_else(|| {
            tonic::Status::new(tonic::Code::InvalidArgument, "File info not found")
        })?;

        let storage = self.storage.lock().await;
        let buf = storage.read(&file_path).await.expect("Failed to read file");
        drop(storage);

        let reply = ReadBlockResponse {
            bytes_read: buf.len() as i64,
            bytes_total: 0,
            block_data: buf,
        };
        Ok(tonic::Response::new(reply))
    }
}