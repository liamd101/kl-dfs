use std::sync::Arc;
use std::thread;

use clap::{Parser, Subcommand};
pub mod block;
pub mod datanode;
use datanode::DataNodeServer;
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;
use tokio::sync::Mutex;

use tokio::time::Duration;

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
}

async fn test(port: String) -> Result<(), Box<dyn std::error::Error>> {
    let namenode = TcpListener::bind("127.0.0.1:8080").await?;

    let datanode = DataNodeServer::new(port);
    datanode.connect_to_namenode("127.0.0.1:8080").await?;

    let datanode_server = Arc::new(Mutex::new(datanode));

    let datanode_server_clone = datanode_server.clone();
    tokio::spawn(async move {
        let locked_server = datanode_server_clone.lock().await;
        locked_server.send_heartbeat_loop().await;
    });

    if let Ok((mut stream, _)) = namenode.accept().await {
        loop {
            let mut buf = [0; 32];
            match stream.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => {
                    println!("read {} bytes", n);
                    println!("{:?}", buf);
                }
                Err(e) => {
                    println!("read failed = {:?}", e);
                    break;
                }
            }
        }
    }

    thread::sleep(Duration::from_secs(6));

    Ok(())
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    match args.command {
        Command::Datanode { port } => {
            let _ = test(port).await;
        }
        Command::Namenode {} => todo!(),
    }
}
