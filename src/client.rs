#![allow(dead_code, unused_variables, unused_imports)]
use crate::proto::data_node_protocols_client::DataNodeProtocolsClient;
use crate::proto::{
    client_protocols_client::ClientProtocolsClient, ClientInfo, CreateFileRequest,
    CreateFileResponse, DeleteFileRequest, DeleteFileResponse, FileInfo, ReadFileRequest,
    ReadFileResponse, SystemInfoRequest, SystemInfoResponse, UpdateFileRequest, UpdateFileResponse,
};
use tokio::io::{self, AsyncBufReadExt, AsyncWriteExt};

use tonic::server;
use tonic::{transport::Channel, Request, Response, Status};

pub struct Client {
    user_id: i64,
    namenode_addr: String, // rpc address: IP & port as a string
    client_addr: String,
    client_info: ClientInfo,
}

impl Client {
    pub fn new(id: i64, name_port: u16, client_port: u16) -> Self {
        Client {
            user_id: id,
            namenode_addr: format!("127.0.0.1:{}", name_port),
            client_addr: format!("127.0.0.1:{}", client_port),
            client_info: ClientInfo { uid: id },
        }
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
                                file_size: 4096,
                            };
                            let request = Request::new(CreateFileRequest {
                                client: Some(self.client_info.clone()),
                                file_info: Some(file.clone()),
                            });
                            let response = client.create_file(request).await?;
                            println!("Response: {:?}", response);

                            let response = response.into_inner();
                            let datanode_addr = response.datanode_address;

                            println!("Connecting to datanode: {}", datanode_addr);
                            let channel =
                                Channel::from_shared(format!("http://{}", datanode_addr))
                                    .unwrap()
                                    .connect()
                                    .await?;

                            let mut datanode_client = DataNodeProtocolsClient::new(channel);
                            println!("Connected to datanode: {}", datanode_addr);

                            let request = Request::new(CreateFileRequest {
                                client: Some(self.client_info.clone()),
                                file_info: Some(file.clone()),
                            });
                            let response = datanode_client.create_file(request).await?;
                            println!("Response: {:?}", response);
                        }
                    }

                    "update" => {
                        if let Some(file_path) = iter.next() {
                            let file = FileInfo {
                                file_path: file_path.to_string(), // so far just flat file system, no directories; this is name
                                file_size: 4096,
                            };
                            let request = Request::new(UpdateFileRequest {
                                client: Some(self.client_info.clone()),
                                file_info: Some(file),
                            });
                            let response = client.update_file(request).await?;
                            println!("Response: {:?}", response);
                        }
                    }
                    "delete" => {
                        if let Some(file_path) = iter.next() {
                            let file = FileInfo {
                                file_path: file_path.to_string(), // so far just flat file system, no directories; this is name
                                file_size: 4096,
                            };
                            let request = Request::new(DeleteFileRequest {
                                client: Some(self.client_info.clone()),
                                file_info: Some(file),
                            });
                            let response = client.delete_file(request).await?;
                            println!("Response: {:?}", response);
                        }
                    }
                    "read" => {
                        if let Some(file_path) = iter.next() {
                            let file = FileInfo {
                                file_path: file_path.to_string(), // so far just flat file system, no directories; this is name
                                file_size: 4096,
                            };

                            let request = Request::new(ReadFileRequest {
                                client: Some(self.client_info.clone()),
                                file_info: Some(file),
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
        }

        Ok(())
    }
}
