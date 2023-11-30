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

    pub fn write(&mut self, data: Vec<u8>) {
        self.data = data;
    }
}
