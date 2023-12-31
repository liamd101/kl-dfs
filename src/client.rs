use std::error::Error;
use std::fmt;
use std::fs::File;
use std::io::Read;
use std::net::SocketAddr;

use crate::proto::{
    client_protocols_client::ClientProtocolsClient,
    data_node_protocols_client::DataNodeProtocolsClient, BlockInfo, DeleteBlockRequest,
    EditBlockRequest, FileInfo, FileRequest, NodeStatus, SystemInfoRequest,
};

use tokio::io::{self, AsyncBufReadExt, AsyncWriteExt};
use tonic::{transport::Channel, Request};

impl fmt::Display for NodeStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}\t\t{}",
            self.node_address,
            if self.is_online { "Online" } else { "Offline" }
        )
    }
}
pub struct Client {
    namenode_client: ClientProtocolsClient<Channel>,
    block_size: usize,
}

impl Client {
    pub async fn new(name_port: u16, block_size: usize) -> Result<Self, Box<dyn Error>> {
        let namenode_addr = SocketAddr::from(([127, 0, 0, 1], name_port));
        let namenode_addr = format!("http://{}", namenode_addr);
        let channel = Channel::from_shared(namenode_addr)
            .unwrap()
            .connect()
            .await?;

        let client = ClientProtocolsClient::new(channel);

        Ok(Client {
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

        const ANSI_BOLD: &str = "\x1b[1m";
        const ANSI_RESET: &str = "\x1b[0m";

        // shell implementation
        loop {
            stdout.write_all(b"> ").await?;
            stdout.flush().await?;

            let mut input = String::new();
            reader.read_line(&mut input).await?;

            let input = input.trim().to_lowercase();

            let mut iter = input.split_whitespace();
            if let Some(command) = iter.next() {
                match command {
                    "system_checkup" => {
                        let request = tonic::Request::new(SystemInfoRequest {});
                        let response = self.namenode_client.get_system_status(request).await?;
                        let response = response.into_inner();
                        let namenode_status = response.namenode.unwrap_or_default();
                        let datanode_statuses = response.nodes;

                        let num_online = datanode_statuses
                            .iter()
                            .filter(|node| node.is_online)
                            .count();
                        let num_offline = datanode_statuses.len() - num_online;

                        println!("{}Node Type\tIP Address\t\tStatus{}", ANSI_BOLD, ANSI_RESET);
                        println!("Namenode\t{}", namenode_status);
                        for node in datanode_statuses {
                            println!("Datanode\t{}", node);
                        }
                        println!(
                            "\n{}Summary{}: {} online, {} offline\n",
                            ANSI_BOLD, ANSI_RESET, num_online, num_offline
                        );
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
                    "exit" => {
                        break;
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

        let request = Request::new(FileRequest {
            file_info: Some(file.clone()),
        });

        let response = match self.namenode_client.read_file(request).await {
            Ok(response) => response,
            Err(e) => return Err(Box::new(e)),
        };

        let response = response.into_inner();
        let block_addrs = response.datanode_addrs;

        let mut buffer = Vec::<u8>::with_capacity(self.block_size);

        if block_addrs.is_empty() {
            println!("File {} does not exist", file_path);
            return Ok(());
        }

        println!("Reading file: {}", file_path);
        for (block_id, blocks) in block_addrs.into_iter().enumerate() {
            let datanode_addr = &blocks.nodes[0];

            let mut datanode_client = self.create_client(datanode_addr).await?;

            let block_name = format!("{}_{}", file_path, block_id);
            let file_info = FileInfo {
                file_path: block_name,
                file_size: 0,
            };
            let request = Request::new(FileRequest {
                file_info: Some(file_info),
            });

            let response = match datanode_client.read_file(request).await {
                Ok(response) => response,
                Err(e) => return Err(Box::new(e)),
            };

            let block_data = response.into_inner().block_data;
            buffer.extend_from_slice(&block_data);
            if buffer.len() >= self.block_size {
                print!("{}", String::from_utf8_lossy(&buffer)); // i think there's a better way
                                                                // to do this
                buffer.clear();
            }
        }
        print!("{}", String::from_utf8_lossy(&buffer));

        Ok(())
    }

    async fn handle_delete(&mut self, file_path: &str) -> Result<(), Box<dyn Error>> {
        let file = FileInfo {
            file_path: file_path.to_string(),
            file_size: 4096,
        };
        let request = Request::new(FileRequest {
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
            let mut datanode_client = self.create_client(datanode_addr).await?;

            let block_name = format!("{}_{}", file_path, block_id);
            let request = Request::new(DeleteBlockRequest { block_name });
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
        let request = Request::new(FileRequest {
            file_info: Some(file),
        });
        let response = match self.namenode_client.update_file(request).await {
            Ok(response) => response,
            Err(e) => return Err(Box::new(e)),
        };

        let response = response.into_inner();
        let block_addrs = &response.datanode_addrs;

        for (block_id, blocks) in block_addrs.iter().enumerate() {
            println!("Updating block {} of {}", block_id, file_path);
            let datanode_addr = &blocks.nodes[0];

            let mut datanode_client = self.create_client(datanode_addr).await?;

            let start = block_id * self.block_size;
            let end = std::cmp::min(file_data.len(), (block_id + 1) * self.block_size);
            let block_size: i64;
            let slice: &[u8];

            if start >= end {
                slice = &[0; 0];
                block_size = 0;
            } else {
                slice = &file_data[start..end];
                block_size = (end - start) as i64;
            }

            let mut block_data = Vec::<u8>::with_capacity(self.block_size);
            let _ = block_data.write(slice).await?;

            let block_info = BlockInfo {
                block_id: block_id as i64,
                block_size,
                block_data,
            };

            let block_name = format!("{}_{}", file_path, block_id);
            let request = Request::new(EditBlockRequest {
                file_name: block_name,
                block_info: Some(block_info.clone()),
            });

            let _ = match datanode_client.update_file(request).await {
                Ok(response) => response,
                Err(e) => return Err(Box::new(e)),
            };
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
        let request = Request::new(FileRequest {
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
            let request = Request::new(EditBlockRequest {
                file_name: block_name,
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
