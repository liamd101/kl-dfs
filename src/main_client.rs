use std::env;

mod dfs_client;
use dfs_client::Client;

// random port 10000 used as default
const DEFAULT_NAMENODE_ADDRESS: &str = "http://localhost:10000";

struct User {
    id: usize,
    name: String,
}

fn main() {
    // [program name, uid, namenode ip address]
    let args: Vec<String> = env::args().collect();

    // Print all command-line arguments
    println!("Command-line arguments for dfs initialization: {:?}", args);

    let uid: i64 = match args.get(1) {
        Some(arg) => match arg.parse() {
            Ok(parsed) => parsed,
            Err(_) => {
                eprintln!("Error parsing UID. Defaulting to 0.");
                0
            }
        },
        None => {
            eprintln!("No UID provided. Defaulting to 0.");
            0
        }
    };

    let namenode_address = match args.get(2) {
        Some(arg) => arg.clone(),
        None => DEFAULT_NAMENODE_ADDRESS.to_string(),
    };


    // instantiate client
    let dfs_client = Client::new(uid, namenode_address);

    // basic shell code to take in cli arguments

}