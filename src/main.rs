use clap::{Parser, Subcommand};
pub mod block;
pub mod datanode;
use datanode::DataNodeServer;
use tokio::io::AsyncReadExt;

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
    let server = DataNodeServer::new(port);
    let listener = server.create_listener().await?;
    server.bind_port().await?;

    match listener.accept().await {
        Ok((socket, _addr)) => {
            let mut buf = [0; 1024];
            let mut stream = socket;
            let n = stream.read(&mut buf).await?;
            if n > 0 {
                println!("Received: {:?}", &buf[..n]);
            }
        }
        Err(e) => println!("couldn't get client: {:?}", e),
    }

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
