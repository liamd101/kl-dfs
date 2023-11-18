#![allow(dead_code)] // remove when implemented :)
pub struct Block {
    id: usize,
    data: Vec<u8>,
}

impl Block {
    pub fn new(id: usize, data: Vec<u8>) -> Self {
        Block { id, data }
    }
}
