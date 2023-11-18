pub mod block;
pub mod datanode;
use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about= None)]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    Datanode {},
    Namenode {},
}

fn main() {
    let args = Args::parse();

    match args.command {
        Command::Datanode {} => todo!(),
        Command::Namenode {} => todo!(),
    }
}
