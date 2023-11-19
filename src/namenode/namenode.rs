

struct NameNode {
    datanodes: Vec<DataNode>,
    blocks: HashMap<u64, Vec<DataNode>>,
    metadata: HashMap<u64, FileMetadata>,
}

