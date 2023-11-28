// use tokio::net::{TcpListener, TcpStream};
// use tokio::io::{AsyncReadExt, AsyncWriteExt};
// use prost::Message;
#[allow(unused_imports)]
use crate::proto::{
    client_protocols_client::ClientProtocolsClient, ClientInfo, CreateFileRequest,
    CreateFileResponse, DeleteFileRequest, DeleteFileResponse, ReadFileRequest, ReadFileResponse,
    SystemInfoRequest, SystemInfoResponse, UpdateFileRequest, UpdateFileResponse,
};
#[allow(unused_imports)]
use tonic::{transport::Channel, Request, Response, Status};
// use network_comms::{
//     client_protocols_client::ClientProtocolsClient,
//     system_info_request::SystemInfoRequest,
//     create_file_request::CreateFileRequest,
//     update_file_request::UpdateFileRequest,
//     delete_file_request::DeleteFileRequest,
//     read_file_request::ReadFileRequest,
//     SystemInfoResponse,
// };

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
        let channel =
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

    // pub async fn ls(&self, path: impl Into<String>) -> Result<Vec<String>> {

    // }

    // // send a write file request to namenode server
    // pub async fn upload(&self, src: &str, dst: impl Into<String>) -> Result<()> {

    // }

    // // send a read file request to namenode server
    // pub async fn download(&self, src: &str, dst: &str) -> Result<()> {

    // }

    pub async fn run_client(&self) {
        // match TcpListener::bind(&self.client_addr).await.unwrap() {
        //     Ok(_) => {
        //         println!("client bound to {}", &self.client_addr);
        //     }
        //     Err(err) => {
        //         println("error binding client");
        //         Err(err)
        //     }
        // }
        // let listener = TcpListener::bind(&self.client_addr).await.unwrap();

        loop {
            // matches CLI and calls get_systeminfo, ls, upload, download, etc.
        }
    }
}
