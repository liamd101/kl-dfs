use std::error::Error;
use std::fs::File;
use std::io::Read;

use std::net::SocketAddr;
use tokio::io::{self, AsyncBufReadExt, AsyncWriteExt};

use crate::proto::{
    client_protocols_client::ClientProtocolsClient,
    data_node_protocols_client::DataNodeProtocolsClient, BlockInfo, ClientInfo, CreateBlockRequest,
    CreateFileRequest, DeleteFileRequest, FileInfo, ReadFileRequest, SystemInfoRequest,
    UpdateBlockRequest, UpdateFileRequest, DeleteBlockRequest,
};

use tonic::{transport::Channel, Request};

pub struct Client {
    client_info: ClientInfo,
    namenode_client: ClientProtocolsClient<Channel>,
    block_size: usize,
}

impl Client {
    pub async fn new(id: i64, name_port: u16, block_size: usize) -> Result<Self, Box<dyn Error>> {
        let namenode_addr = SocketAddr::from(([127, 0, 0, 1], name_port));
        let namenode_addr = format!("http://{}", namenode_addr);
        let channel = Channel::from_shared(namenode_addr)
            .unwrap()
            .connect()
            .await?;

        let client = ClientProtocolsClient::new(channel);

        Ok(Client {
            client_info: ClientInfo { uid: id },
            namenode_client: client,
            block_size,
        })
    }

    async fn create_client(
        &self,
        client_addr: &str,
    ) -> Result<DataNodeProtocolsClient<Channel>, Box<dyn Error>> {
        let client_addr = format!("http://{}", client_addr);
        let channel = Channel::from_shared(client_addr).unwrap().connect().await?;

        Ok(DataNodeProtocolsClient::new(channel))
    }

    pub async fn run_client(&mut self) -> Result<(), Box<dyn std::error::Error>> {
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
                        let response = self.namenode_client.get_system_status(request).await?;
                        println!("Response: {:?}", response);
                    }

                    "create" => {
                        if let Some(file_path) = iter.next() {
                            match self.handle_create(file_path).await {
                                Ok(_) => {}
                                Err(e) => {
                                    println!("Error: {}", e);
                                    continue;
                                }
                            }
                        }
                    }

                    "update" => {
                        if let Some(file_path) = iter.next() {
                            match self.handle_update(file_path).await {
                                Ok(_) => {}
                                Err(e) => {
                                    println!("Error: {}", e);
                                    continue;
                                }
                            }
                        }
                    }

                    "delete" => {
                        if let Some(file_path) = iter.next() {
                            match self.handle_delete(file_path).await {
                                Ok(_) => {}
                                Err(e) => {
                                    println!("Error: {}", e);
                                    continue;
                                }
                            }
                        }
                    }

                    "read" => {
                        if let Some(file_path) = iter.next() {
                            match self.handle_read(file_path).await {
                                Ok(_) => {}
                                Err(e) => {
                                    println!("Error: {}", e);
                                    continue;
                                }
                            }
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

    async fn handle_read(&mut self, file_path: &str) -> Result<(), Box<dyn Error>> {
        // how to get the blocks from the file name??
        let file = FileInfo {
            file_path: file_path.to_string(),
            file_size: 4096,
        };

        let request = Request::new(ReadFileRequest {
            client: Some(self.client_info.clone()),
            file_info: Some(file.clone()),
        });

        let response = match self.namenode_client.read_file(request).await {
            Ok(response) => response,
            Err(e) => return Err(Box::new(e)),
        };

        let response = response.into_inner();
        let block_addrs = response.datanode_addrs;

        for (block_id, blocks) in block_addrs.into_iter().enumerate() {
            let datanode_addr = &blocks.nodes[0];

            println!("Reading from datanode: {}", datanode_addr);
            let mut datanode_client = self.create_client(datanode_addr).await?;

            let block_name = format!("{}_{}", file_path, block_id);
            let file_info = FileInfo {
                file_path: block_name,
                file_size: 0, // not used
            };
            let request = Request::new(ReadFileRequest {
                client: Some(self.client_info.clone()),
                file_info: Some(file_info),
            });

            let response = match datanode_client.read_file(request).await {
                Ok(response) => response,
                Err(e) => return Err(Box::new(e)),
            };

            let data = response.into_inner().block_data;
            println!("File content:\n{}", String::from_utf8_lossy(&data));
        }

        Ok(())
    }

    async fn handle_delete(&mut self, file_path: &str) -> Result<(), Box<dyn Error>> {
        let file = FileInfo {
            file_path: file_path.to_string(), // so far just flat file system, no directories; this is name
            file_size: 4096,
        };
        let request = Request::new(DeleteFileRequest {
            client: Some(self.client_info.clone()),
            file_info: Some(file.clone()),
        });
        let response = match self.namenode_client.delete_file(request).await {
            Ok(response) => response,
            Err(e) => return Err(Box::new(e)),
        };

        let response = response.into_inner();
        let block_addrs = response.datanode_addrs;

        for (block_id, blocks) in block_addrs.into_iter().enumerate() {
            let datanode_addr = &blocks.nodes[0];
            println!("Deleting {} from datanode: {}", file_path, datanode_addr);
            let mut datanode_client = self.create_client(datanode_addr).await?;

            let block_name = format!("{}_{}", file_path, block_id);
            let request = Request::new(DeleteBlockRequest {
                client_info: Some(self.client_info.clone()),
                block_name
            });
            let response = match datanode_client.delete_file(request).await {
                Ok(response) => response,
                Err(e) => return Err(Box::new(e)),
            };

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
        Ok(())
    }

    async fn handle_update(&mut self, file_path: &str) -> Result<(), Box<dyn Error>> {
        let (file_size, file_data) = match File::open(file_path) {
            Ok(file) => {
                let size = file.metadata().unwrap().len() as i64;
                let data = file.bytes().map(|b| b.unwrap()).collect();
                (size, data)
            }
            Err(_) => (0, vec![]),
        };

        let file = FileInfo {
            file_path: file_path.to_string(), // so far just flat file system, no directories; this is name
            file_size,
        };
        let request = Request::new(UpdateFileRequest {
            client: Some(self.client_info.clone()),
            file_info: Some(file),
        });
        let response = match self.namenode_client.update_file(request).await {
            Ok(response) => response,
            Err(e) => return Err(Box::new(e)),
        };

        let response = response.into_inner();
        let block_addrs = &response.datanode_addrs;

        let datanode_addr = &block_addrs[0].nodes[0];

        println!(
            "Writing contents of {} to datanode: {}",
            file_path, datanode_addr
        );
        let mut datanode_client = self.create_client(datanode_addr).await?;

        let block_info = BlockInfo {
            block_id: 0,
            block_size: file_size,
            block_data: file_data,
        };
        let request = Request::new(UpdateBlockRequest {
            file_name: file_path.to_string(),
            client_info: Some(self.client_info.clone()),
            block_info: Some(block_info.clone()),
        });
        let response = match datanode_client.update_file(request).await {
            Ok(response) => response,
            Err(e) => return Err(Box::new(e)),
        };

        if !response.into_inner().success {
            println!("Failed to update file: {}", file_path);
        } else {
            println!("Successfully updated file: {}", file_path);
        }
        Ok(())
    }

    async fn handle_create(&mut self, file_path: &str) -> Result<(), Box<dyn Error>> {
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
        let response = match self.namenode_client.create_file(request).await {
            Ok(response) => response,
            Err(e) => return Err(Box::new(e)),
        };

        let response = response.into_inner();
        let block_addrs = response.datanode_addrs;

        for (block_id, blocks) in block_addrs.into_iter().enumerate() {
            let datanode_addr = &blocks.nodes[0];
            println!(
                "Writing contents of {} to datanode: {}",
                file_path, datanode_addr
            );

            let mut datanode_client = self.create_client(datanode_addr).await?;

            let start = block_id * self.block_size;
            let end = std::cmp::min(file_data.len(), (block_id + 1) * self.block_size);
            let slice = &file_data[start..end];

            let mut block_data = Vec::<u8>::with_capacity(self.block_size);
            let _ = block_data.write(slice).await?;

            let block_info = BlockInfo {
                block_id: block_id as i64,
                block_size: (end - start) as i64,
                block_data,
            };

            let block_name = format!("{}_{}", file_path, block_id);
            let request = Request::new(CreateBlockRequest {
                file_name: block_name,
                client_info: Some(self.client_info.clone()),
                block_info: Some(block_info.clone()),
            });

            let _ = match datanode_client.create_file(request).await {
                Ok(response) => response,
                Err(e) => return Err(Box::new(e)),
            };
        }

        Ok(())
    }
}
