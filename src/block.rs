use crate::proto::BlockInfo;

#[derive(Clone, Debug)]
pub struct Block {
    pub name: String,
    data: Vec<u8>,
}

impl Block {
    pub fn new(name: String, data: Vec<u8>) -> Self {
        Self { name, data }
    }

    pub fn read(&self) -> Vec<u8> {
        self.data.clone()
    }

    pub fn write(&mut self, block_info: BlockInfo) {
        let data_to_write = if block_info.block_data.len() > block_info.block_size as usize {
            &block_info.block_data[..block_info.block_size as usize]
        } else {
            &block_info.block_data
        };

        self.data = data_to_write.to_vec();
    }
}
