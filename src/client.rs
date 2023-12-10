use std::fs::File;
use std::io::Read;

use crate::proto::data_node_protocols_client::DataNodeProtocolsClient;
use crate::proto::CreateBlockRequest;
use crate::proto::{
    client_protocols_client::ClientProtocolsClient, BlockInfo, ClientInfo, CreateFileRequest,
    DeleteFileRequest, FileInfo, ReadFileRequest, SystemInfoRequest, UpdateFileRequest,
};
use tokio::io::{self, AsyncBufReadExt, AsyncWriteExt};

use tonic::{transport::Channel, Request};

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
                            let (file_size, file_data) = match File::open(file_path) {
                                Ok(file) => {
                                    let size = file.metadata().unwrap().len() as i64;
                                    let data = file.bytes().map(|b| b.unwrap()).collect();
                                    (size, data)
                                }
                                Err(_) => (0, vec![]),
                            };

                            let file = FileInfo {
                                file_path: file_path.to_string(),
                                file_size,
                            };
                            let request = Request::new(CreateFileRequest {
                                client: Some(self.client_info.clone()),
                                file_info: Some(file.clone()),
                            });
                            let response = client.create_file(request).await?;

                            let response = response.into_inner();
                            let datanode_addr = response.datanode_addr;

                            println!(
                                "Writing contents of {} to datanode: {}",
                                file_path, datanode_addr
                            );
                            let channel = Channel::from_shared(format!("http://{}", datanode_addr))
                                .unwrap()
                                .connect()
                                .await?;

                            let mut datanode_client = DataNodeProtocolsClient::new(channel);

                            let block_info = BlockInfo {
                                block_id: 0,
                                block_size: file_size,
                                block_data: file_data,
                            };
                            let request = Request::new(CreateBlockRequest {
                                file_name: file_path.to_string(),
                                client_info: Some(self.client_info.clone()),
                                block_info: Some(block_info.clone()),
                            });
                            let response = datanode_client.create_file(request).await?;

                            if !response.into_inner().success {
                                println!("Failed to create file: {}", file_path);
                            } else {
                                println!("Successfully created file: {}", file_path);
                            }
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
                                file_info: Some(file.clone()),
                            });
                            let response = client.delete_file(request).await?;

                            let response = response.into_inner();
                            let datanode_addrs = response.datanode_addr;

                            for datanode_addr in datanode_addrs {
                                println!("Deleting {} from datanode: {}", file_path, datanode_addr);
                                let channel =
                                    Channel::from_shared(format!("http://{}", datanode_addr))
                                        .unwrap()
                                        .connect()
                                        .await?;

                                let mut datanode_client = DataNodeProtocolsClient::new(channel);

                                let request = Request::new(DeleteFileRequest {
                                    client: Some(self.client_info.clone()),
                                    file_info: Some(file.clone()),
                                });
                                let response = datanode_client.delete_file(request).await?;

                                if !response.into_inner().success {
                                    println!(
                                        "Failed to delete {} from datanode: {}",
                                        file_path, datanode_addr,
                                    );
                                } else {
                                    println!(
                                        "Successfully deleted {} from datanode: {}",
                                        file_path, datanode_addr,
                                    );
                                }
                            }
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
                                file_info: Some(file.clone()),
                            });

                            let response = client.read_file(request).await?;

                            let response = response.into_inner();
                            let datanode_addr = &response.datanode_addr[0];

                            println!("Reading from datanode: {}", datanode_addr);
                            let channel = Channel::from_shared(format!("http://{}", datanode_addr))
                                .unwrap()
                                .connect()
                                .await?;

                            let mut datanode_client = DataNodeProtocolsClient::new(channel);

                            let request = Request::new(ReadFileRequest {
                                client: Some(self.client_info.clone()),
                                file_info: Some(file.clone()),
                            });

                            let response = datanode_client.read_file(request).await?;
                            let data = response.into_inner().block_data;
                            println!("File content: {}", String::from_utf8_lossy(&data));
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
