#![allow(dead_code)] // remove when implemented :)
#[derive(Clone, Debug)]
pub struct Block {
    id: usize,
    data: Vec<u8>,
}

impl Block {
    pub fn new(id: usize, data: Vec<u8>) -> Self {
        Self { id, data }
    }
}
