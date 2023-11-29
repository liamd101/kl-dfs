use crate::proto::FileInfo;
// use tokio::net::{TcpListener, TcpStream};
// use tokio::io::{AsyncReadExt, AsyncWriteExt};
// use prost::Message;
#[allow(unused_imports)]
use crate::proto::{
    client_protocols_client::ClientProtocolsClient, ClientInfo, CreateFileRequest,
    CreateFileResponse, DeleteFileRequest, DeleteFileResponse, ReadFileRequest, ReadFileResponse,
    SystemInfoRequest, SystemInfoResponse, UpdateFileRequest, UpdateFileResponse,
};
// use std::io::{self, Write};
use tokio::io::{self, AsyncBufReadExt, AsyncWriteExt};
// use clap::{App, Arg};

#[allow(unused_imports)]
use tonic::server;
#[allow(unused_imports)]
use tonic::{transport::Channel, Request, Response, Status};

pub struct Client {
    user_id: i64,
    namenode_addr: String, // rpc address: IP & port as a string
    client_addr: String,
    client_info :ClientInfo
}

impl Client {
    pub fn new(id: i64, name_port: String, client_port: String) -> Self {
        Client {
            user_id: id,
            namenode_addr: format!("127.0.0.1:{}", name_port),
            client_addr: format!("127.0.0.1:{}", client_port),
            client_info: ClientInfo { uid: id }
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
        // let request = tonic::Request::new(SystemInfoRequest::default());
        // let response = client.get_system_status(request).await?;
        // // Process the response
        // println!("Response: {:?}", response);

        let mut stdout = io::stdout();
        let stdin = io::stdin();
        let mut reader = io::BufReader::new(stdin);

        // let args = [Arg::new("command")
        //                             .required(true)
        //                             .index(1),
        //                             Arg::new("arguments")
        //                             .required(false)
        //                             .multiple(true)];

        // shell implementation
        loop {
            stdout.write_all(b"> ").await?;
            stdout.flush().await?;

            let mut input = String::new();
            reader.read_line(&mut input).await?;

            let input = input.trim().to_lowercase();
            if input == "exit" {
                break;
            }

            let mut iter = input.split_whitespace();
            if let Some(command) = iter.next() {
                match command {
                    "system_checkup" => {
                        let request = tonic::Request::new(SystemInfoRequest {
                            client: Some(self.client_info.clone()),
                        });
                        let response = client.get_system_status(request).await?;
                        println!("Response: {:?}", response);
                    }
                    "create" => {
                        if let Some(file_path) = iter.next() {
                            let file = FileInfo {
                                file_path: file_path.to_string(), // so far just flat file system, no directories; this is name
                                file_size: 4096
                            };
                            let request = Request::new(CreateFileRequest {
                                client: Some(self.client_info.clone()),
                                file_info: Some(file)
                            });
                            let response = client.create_file(request).await?;
                            println!("Response: {:?}", response);
                        }
                    }
                    "update" => {
                        if let Some(file_path) = iter.next() {
                            let file = FileInfo {
                                file_path: file_path.to_string(), // so far just flat file system, no directories; this is name
                                file_size: 4096
                            };
                            let request = Request::new(UpdateFileRequest {
                                client: Some(self.client_info.clone()),
                                file_info: Some(file)
                            });
                            let response = client.update_file(request).await?;
                            println!("Response: {:?}", response);
                        }
                    }
                    "delete" => {
                        if let Some(file_path) = iter.next() {
                            let file = FileInfo {
                                file_path: file_path.to_string(), // so far just flat file system, no directories; this is name
                                file_size: 4096
                            };
                            let request = Request::new(DeleteFileRequest {
                                client: Some(self.client_info.clone()),
                                file_info: Some(file)
                            });
                            let response = client.delete_file(request).await?;
                            println!("Response: {:?}", response);
                        }
                    }
                    "read" => {
                        if let Some(file_path) = iter.next() {
                            let file = FileInfo {
                                file_path: file_path.to_string(), // so far just flat file system, no directories; this is name
                                file_size: 4096
                            };
                            let request = Request::new(ReadFileRequest {
                                client: Some(self.client_info.clone()),
                                file_info: Some(file)
                            });
                            let response = client.read_file(request).await?;
                            println!("Response: {:?}", response);
                        }
                    }
                    _ => {
                        println!("Invalid Command.");
                        continue;
                    }
                };
            }

            // Create a gRPC request based on user input
            // let request = tonic::Request::new(YourRequestType {
            //     // Set fields based on user input
            //     // For example, if your request has a string field 'command'
            //     command: input.to_string(),
            // });

            // // Make an RPC call
            // match client.your_rpc_method(request).await {
            //     Ok(response) => {
            //         // Process the response
            //         println!("Response: {:?}", response);
            //     }
            //     Err(e) => {
            //         eprintln!("Error: {:?}", e);
            //     }
            // }
        }

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
