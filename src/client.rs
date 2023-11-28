// use tokio::net::{TcpListener, TcpStream};
// use tokio::io::{AsyncReadExt, AsyncWriteExt};
// use prost::Message;
#[allow(unused_imports)]
use crate::proto::{
    client_protocols_client::ClientProtocolsClient, ClientInfo, CreateFileRequest,
    CreateFileResponse, DeleteFileRequest, DeleteFileResponse, ReadFileRequest, ReadFileResponse,
    SystemInfoRequest, SystemInfoResponse, UpdateFileRequest, UpdateFileResponse,
};
use tonic::server;
#[allow(unused_imports)]
use tonic::{transport::Channel, Request, Response, Status};


pub struct Client {
    user_id: i64,
    namenode_addr: String, // rpc address: IP & port as a string
    client_addr: String,
}

impl Client {
    pub fn new(id: i64, name_port: String, client_port: String) -> Self {
        Client {
            user_id: id,
            namenode_addr: format!("127.0.0.1:{}", name_port),
            client_addr: format!("127.0.0.1:{}", client_port),
        }
    }

    // sends a system_info_request to the namenode in self.namenode_address
    pub async fn get_system_status(&self) -> Result<SystemInfoResponse, tonic::Status> {
        let _channel =
            tonic::transport::Channel::from_shared(format!("http://{}", self.namenode_addr))
                .map_err(|e| {
                    tonic::Status::internal(format!("Failed to create channel: {:?}", e))
                })?;

        // let client = ClientProtocolsClient::new(channel);

        // let request = tonic::Request::new(SystemInfoRequest {
        //     client: Some(ClientInfo { uid: self.user_id }),
        // });

        // let response = client.get_system_status(request).await?;

        // Ok(response.into_inner())
        unimplemented!()
    }

    pub async fn run_client(&self) -> Result<(), Box<dyn std::error::Error>> {
        let channel = Channel::from_shared(format!("http://{}", self.namenode_addr))
            .unwrap()
            .connect()
            .await?;

        let mut client = ClientProtocolsClient::new(channel);
        let request = tonic::Request::new(SystemInfoRequest::default());

        let response = client.get_system_status(request).await?;

        // Process the response
        println!("Response: {:?}", response);

        // shell implementation
        // loop {

        // }
        
        Ok(())
    }
}

// struct ClientService {
//     client: Client,
// }

// #[tonic::async_trait]
// impl ClientProtocols for ClientService {

//     async fn get_system_status(
//         &self,
//         request: Request<SystemInfoRequest>,
//     ) -> Result<Response<SystemInfoResponse>, Status> {
//         unimplemented!()
//     }
    

// }