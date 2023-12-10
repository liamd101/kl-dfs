use std::error::Error;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::interval;

use crate::proto::data_node_protocols_server::{DataNodeProtocols, DataNodeProtocolsServer};
use crate::proto::{
    hearbeat_protocol_client::HearbeatProtocolClient, CreateBlockResponse, CreateFileRequest,
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

    pub async fn run_dataserver(&self) -> Result<(), Box<dyn std::error::Error>> {
        let heartbeat_status = self.send_heartbeat_loop();
        let service_status = self.run_service();
        
        tokio::select! {
            result = heartbeat_status => {
                if let Err(_) = result {
                    return result;
                }
            }
            result = service_status => {
                if let Err(_) = result {
                    return result;
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
        let channel = Channel::from_shared(format!("http://{}", self.namenode_addr.to_string()))
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
        request: tonic::Request<CreateFileRequest>,
    ) -> Result<tonic::Response<CreateBlockResponse>, tonic::Status> {
        println!("Entered create_file");

        let request = request.into_inner();
        let FileInfo {
            file_path,
            file_size,
        } = request.file_info.expect("File info not found");

        let mut storage = self.storage.lock().await;
        storage
            .create(&file_path)
            .await
            .expect("Failed to create file");

        let reply = CreateBlockResponse { success: true };

        Ok(tonic::Response::new(reply))
    }

    async fn update_file(
        &self,
        request: tonic::Request<UpdateFileRequest>,
    ) -> Result<tonic::Response<UpdateBlockResponse>, tonic::Status> {
        let request = request.into_inner();

        let reply = UpdateBlockResponse { success: true };
        Ok(tonic::Response::new(reply))
    }

    async fn delete_file(
        &self,
        request: tonic::Request<DeleteFileRequest>,
    ) -> Result<tonic::Response<DeleteBlockResponse>, tonic::Status> {
        let request = request.into_inner();

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
            file_size,
        } = request.file_info.expect("File info not found");

        let storage = self.storage.lock().await;
        let buf = storage.read(&file_path).await.expect("Failed to read file");

        let reply = ReadBlockResponse {
            bytes_read: 0,
            bytes_total: 0,
            block_data: buf,
        };
        Ok(tonic::Response::new(reply))
    }
}
