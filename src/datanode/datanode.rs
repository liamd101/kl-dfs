use chrono::Utc;
use std::error::Error;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;
use tokio::time::interval;

use crate::proto::data_node_protocol_server::DataNodeProtocol;
use crate::proto::EmptyMessage;
use crate::proto::HeartbeatMessage;

use crate::datanode::storage::Storage;

/// Server that runs a datanode
#[derive(Clone)]
pub struct DataNodeServer {
    /// TCP address of the datanode
    pub datanode_addr: SocketAddr,

    /// Block storage of the datanode
    pub storage: Storage,

    /// Connection to the namenode
    pub namenode_addr: SocketAddr,
}

impl DataNodeServer {
    pub fn new(port: u16) -> Self {
        let datanode_addr: SocketAddr =
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port);
        let namenode_addr: SocketAddr =
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 3000);
        DataNodeServer {
            datanode_addr,
            storage: Storage::new(),
            namenode_addr,
        }
    }

    pub async fn run_dataserver(&self) -> Result<(), Box<dyn std::error::Error>> {
        let heartbeat_status = self.send_heartbeat_loop();

        tokio::select! {
            result = heartbeat_status => {
                if let Err(_) = result {
                    return result;
                }
            }
        }
        Ok(())
    }

    pub async fn send_heartbeat_loop(&self) -> Result<(), Box<dyn Error>> {
        let mut interval = interval(Duration::from_secs(5));
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let request = tonic::Request::new(EmptyMessage{});
                    self.heartbeat(request).await?;
                }
            }
        }
    }
}

#[tonic::async_trait]
impl DataNodeProtocol for DataNodeServer {
    async fn heartbeat(
        &self,
        _: tonic::Request<EmptyMessage>,
    ) -> Result<tonic::Response<HeartbeatMessage>, tonic::Status> {
        let heartbeat = HeartbeatMessage {
            node_id: self.datanode_addr.to_string(),
            timestamp: Utc::now().timestamp(),
        };
        Ok(tonic::Response::new(heartbeat))
    }
}
