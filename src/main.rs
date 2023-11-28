use std::sync::Arc;

use clap::{Parser, Subcommand};
pub mod block;
pub mod datanode;
use datanode::DataNodeServer;
use tokio::net::TcpListener;
use tokio::sync::Mutex;

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
    Datanode { port: String },
    Namenode {},
    NamenodeDev {},
}

async fn run_datanode_server(port: String) {
    let _namenode = TcpListener::bind("127.0.0.1:8080").await;

    let datanode = DataNodeServer::new(port);
    match datanode.connect_to_namenode("127.0.0.1:8080").await {
        Ok(_) => (),
        Err(e) => {
            println!("Error connecting to namenode: {}", e);
            return;
        }
    }

    let datanode_server = Arc::new(Mutex::new(datanode));

    let datanode_server_clone = datanode_server.clone();
    tokio::spawn(async move {
        let locked_server = datanode_server_clone.lock().await;
        locked_server.send_heartbeat_loop().await;
    });
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    match args.command {
        Command::Datanode { port } => {
            let nport = port.clone();
            tokio::spawn(async move {
                run_datanode_server(nport.clone()).await;
            });

            println!("Datanode running on port {}", port);
        },
        Command::Namenode {} => todo!(),
        Command::NamenodeDev {} => {
            // hard coded ports for developing initial client-namenode comms
            let nameserver = NameNodeServer::new("4000".to_string());
            let _client = Client::new(1, "4000".to_string(), "4200".to_string());

            let _ = nameserver.run_nameserver().await;

            
        }
    }
}
