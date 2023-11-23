use crate::either::{
    Either,
    Either::{Left, Right},
};
use crate::segment::{block::ValueBlock, index::IndexBlock};
use quick_cache::sync::Cache;
use std::sync::Arc;

type Key = (u8, Vec<u8>);
type Item = Either<Arc<ValueBlock>, Arc<IndexBlock>>;

pub struct BlockCache {
    data: Cache<Key, Item>,
    //  capacity: usize,
}

impl BlockCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            data: Cache::new(capacity),
            //  capacity,
        }
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn insert_disk_block(&self, key: Vec<u8>, value: Arc<ValueBlock>) {
        self.data.insert((0, key), Left(value));
    }

    pub fn insert_index_block(&self, key: Vec<u8>, value: Arc<IndexBlock>) {
        self.data.insert((1, key), Right(value));
    }

    pub fn get_disk_block(&self, key: &[u8]) -> Option<Arc<ValueBlock>> {
        let key = (0, key.to_vec());
        let item = self.data.get(&key)?;
        Some(item.left().clone())
    }

    pub fn get_index_block(&self, key: &[u8]) -> Option<Arc<IndexBlock>> {
        let key = (1, key.to_vec());
        let item = self.data.get(&key)?;
        Some(item.right().clone())
    }
}
