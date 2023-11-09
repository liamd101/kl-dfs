# cis1905-project

in progress...

# TODO
for now and for simplicity, each file is one block.
- [ ] Client
  - [ ] CLI argument parsing
  - [ ] Correctly formatting and sending requests with warp
- [ ] DataNode
  - [ ] Processes r/w/d request from the client.
  - [ ] Implement writing/deleting a data block from block id
  - [ ] Implement reading a data blcok from block id
  - [ ] Sends periodic heartbeat messages to the name node
- [ ] NameNode
  - [ ] Data structure for storing metadata about files (metadata: file names (identifier), which data nodes data is on, date last modified, date created, list of users with their permissions, etc..)
  - [ ] Implement checking user permissions for given file.
  - [ ] Implement hashing for rerouting client to a datanode. hashing also used for determining which data node to create a file on.
  - [ ] LRU implementation for caching. Also caching data structure, cache is for the data node that a recently used file is stored on.
  - [ ] Implement returning correct data node id and block id on that data node to the client.
  - [ ] Can receive and check heartbeak messages from data nodes.
    - [ ] Implement replication in the case of a failed data node.

References:
https://github.com/cjlongoria/LimeWireLite
https://github.com/Elaine919/GKD/tree/master
https://github.com/radogost/ucz-dfs
https://github.com/xfbs/cloudfs [actually seems useful - does hashing, lru caching]
https://github.com/ProjectInitiative/rfs/tree/main
https://github.com/m4tx/offs [uses client side caching]

https://medium.com/@dhammikasamankumara/what-is-hadoop-distributed-file-system-hdfs-36a3503f9c60
https://hadoop.apache.org/docs/r1.2.1/hdfs_design.html#NameNode+and+DataNodes [architecture of HDFS]
