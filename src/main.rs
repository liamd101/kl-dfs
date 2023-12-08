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
    Heartbeat_test {},
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let namenode_port = 3000;
    let client_port = 3030;
    let datanode_port = 7000;

    match args.command {
        Command::Datanode { port } => {
            // let dataserver = DataNodeServer::new(port);
            // match dataserver.run_dataserver().await {
            //     Ok(_) => println!("DataNode ran successfully"),
            //     Err(err) => println!("Error: {}", err),
            // }
        }

        Command::Namenode {} => {
            let nameserver = NameNodeServer::new(namenode_port);
            let _ = nameserver.run_nameserver().await;
        }

        Command::Client {} => {
            let client = Client::new(1, namenode_port, client_port);
            match client.run_client().await {
                Ok(_) => println!("Client ran successfully"),
                Err(err) => println!("Client Error: {}", err),
            }
        }

        Command::Heartbeat_test {  } => {
            let dataserver = DataNodeServer::new(datanode_port, namenode_port);
            let _ = dataserver.run_dataserver().await;
        }
    }
}
