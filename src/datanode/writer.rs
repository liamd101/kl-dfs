#![allow(dead_code, unused_imports, unused_variables)]
use std::error::Error;
use std::net::SocketAddr;

use crate::proto::client_protocols_client::ClientProtocolsClient;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tonic::transport::Channel;

/// Struct that handles writing to a file
pub struct Writer {
    /// Connection to the namenode
    datanode_client: ClientProtocolsClient<Channel>,

    /// Path of the file to write to
    path: String,

    /// Whether the file has been started or not
    file_started: bool,
}

impl Writer {
    pub async fn new(datanode_addr: SocketAddr, path: String) -> Result<Self, Box<dyn Error>> {
        let datanode_client =
            ClientProtocolsClient::connect(format!("http://{}", datanode_addr)).await?;

        Ok(Writer {
            datanode_client,
            path,
            file_started: false,
        })
    }

    pub async fn write(&mut self, datanode: SocketAddr) -> Result<(), Box<dyn Error>> {
        let mut datanode = TcpStream::connect(datanode).await?;
        let mut buf = vec![];
        datanode.write_all(&mut buf).await?;
        buf.clear();

        Ok(())
    }
}
