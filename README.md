# cis1905-project: Distributed Rust Filesystem
This project is a distributed filesystem implemented in Rust, implementing replicable storage across multiple nodes.

# Getting Started
## Starting Nodes
1. Start a namenode by running `cargo run namenode`. This starts a namenode on localhost port 3000.
2. Start a datanode by running `cargo run datanode [-port]`. This starts a datanode on the specified localhost port.
3. Start a client instance by running `cargo run client`. This provides access to a shell to execute commands.
By default, the system has a replication factor of 3 (each block will be stored on 3 datanodes, and this can be changed by passing in hyperparameters). New datanodes automatically connect to and are reigstered by the namenode by sending na initial heartbeat message. Currently, if a datanode shuts down the client will be able to still retrieve the file from a live datanode containing the file.

## Available Commands
- `system_checkup`: retrieve the statuses of all the nodes in the system.
- `create [-file]`: if `file` is an already existing file on your local machine, that file will be written to some number of datanodes within the system.
- `update [-file]`: updates every instance of `file` on a datanode with the contents of that file on your local machine.
- `delete [-file]`: deletes `file` from every datanode in the system if it exists
- `read [-file]`: retrieves the contents of `file` fro one of the datanodes and prints it to the terminal
- `exit`: gracefully exits the client shell

# TODO
- [x] Client
  - [x] CLI argument parsing
  - [x] Correctly formatting and sending requests
- [x] DataNode
  - [x] Processes r/w/d request from the client.
  - [x] Implement writing/deleting a data block from block id
  - [x] Implement reading a data blcok from block id
  - [x] Sends periodic heartbeat messages to the name node
- [ ] NameNode
  - [x] Data structure for storing metadata about files (metadata: file names (identifier), which data nodes data is on, date last modified, date created, list of users with their permissions, etc..)
  - [ ] Implement checking user permissions for given file.
  - [x] Implement hashing for rerouting client to a datanode. hashing also used for determining which data node to create a file on.
  - [ ] LRU implementation for caching. Also caching data structure, cache is for the data node that a recently used file is stored on.
  - [x] Implement returning correct data node id and block id on that data node to the client.
  - [x] Can receive and check heartbeak messages from data nodes.
    - [ ] Implement replication in the case of a failed data node.

# Next Steps
- Handle datanode shutdowns from the namenode side. Need to remove dead datanodes from the system so that new file creations aren't mapped to those datanodes. Also need to re-replicate the blocks on the crashed datanode to another one to maintain the replication-factor guarantee.
- Implement checks for ensuring a r/w/u/d is fully completed. This can be done by storing a struct of in-progress requests and having the client send a message to the namenode when the r/w/u/d to the datanode(s) is completed.

References:
- https://github.com/xfbs/cloudfs [hashing, lru caching]
- https://github.com/m4tx/offs [uses client side caching]
- https://medium.com/@dhammikasamankumara/what-is-hadoop-distributed-file-system-hdfs-36a3503f9c60
- https://hadoop.apache.org/docs/r1.2.1/hdfs_design.html#NameNode+and+DataNodes [architecture of HDFS]
