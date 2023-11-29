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
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    match args.command {
        Command::Datanode { port } => {
            let dataserver = DataNodeServer::new(port);
            match dataserver.run_dataserver().await {
                Ok(_) => println!("DataNode ran successfully"),
                Err(err) => println!("Error: {}", err),
            }
        }

        Command::Namenode {} => {
            let nameserver = NameNodeServer::new("3000".to_string());
            let _ = nameserver.run_nameserver().await;
        }

        Command::Client {} => {
            let client = Client::new(1, "4000".to_string(), "4200".to_string());
            match client.run_client().await {
                Ok(_) => println!("Client ran successfully"),
                Err(err) => println!("Error: {}", err),
            }
        }
    }
}
