use clap::{Parser, Subcommand};
pub mod block;

pub mod datanode;
use datanode::DataNodeServer;
pub mod client;
use client::Client;
pub mod namenode;
use namenode::NameNodeServer;

pub mod proto {
    tonic::include_proto!("network_comms");
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about= None)]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    Datanode { port: u16 },
    Namenode {},
    Client {},
    HeartbeatTest { port: u16 },
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let namenode_port = 3000;
    let block_size = 3;
    let replication_factor = 3;

    match args.command {
        Command::Datanode { port } => {
            let dataserver = DataNodeServer::new(port, namenode_port);
            let _ = dataserver.run_dataserver().await;
        }

        Command::Namenode {} => {
            let nameserver = NameNodeServer::new(namenode_port, replication_factor, block_size);
            let _ = nameserver.run_nameserver().await;
        }

        Command::Client {} => {
            let mut client = Client::new(1, namenode_port, block_size)
                .await
                .expect("Client failed");
            match client.run_client().await {
                Ok(_) => println!("Client ran successfully"),
                Err(err) => println!("Client Error: {}", err),
            }
        }

        Command::HeartbeatTest { port } => {
            let dataserver = DataNodeServer::new(port, namenode_port);
            let _ = dataserver.run_dataserver().await;
        }
    }
}
