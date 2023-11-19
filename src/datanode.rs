struct Block {
    id: usize,
    data: Vec<u8>,
}

struct DataNode {
    id: usize,
    blocks: HashMap<usize, Block>,
}

impl DataNode {
    fn new() -> Self {
        DataNode {
            blocks: HashMap::new(),
        }
    }

    fn write_block(&mut self, block: Block) {
        self.blocks.insert(block.id, block);
    }

    fn read_block(&self, id: usize) -> Option<&Block> {
        self.blocks.get(&id)
    }
}
