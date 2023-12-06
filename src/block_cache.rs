use crate::segment::{block::ValueBlock, index::IndexBlock};
use crate::{
    either::{
        Either,
        Either::{Left, Right},
    },
    value::UserKey,
};
use quick_cache::sync::Cache;
use std::sync::Arc;

// Type (disk or index), Segment ID, Block key
type Key = (u8, String, UserKey);
type Item = Either<Arc<ValueBlock>, Arc<IndexBlock>>;

pub struct BlockCache {
    data: Cache<Key, Item>,
    capacity: usize,
}

impl BlockCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            data: Cache::new(capacity),
            capacity,
        }
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn insert_disk_block(&self, segment_id: String, key: UserKey, value: Arc<ValueBlock>) {
        if self.capacity > 0 {
            self.data.insert((0, segment_id, key), Left(value));
        }
    }

    pub fn insert_index_block(&self, segment_id: String, key: UserKey, value: Arc<IndexBlock>) {
        if self.capacity > 0 {
            self.data.insert((1, segment_id, key), Right(value));
        }
    }

    pub fn get_disk_block(&self, segment_id: String, key: &[u8]) -> Option<Arc<ValueBlock>> {
        let key = (0, segment_id, key.to_vec().into());
        let item = self.data.get(&key)?;
        Some(item.left().clone())
    }

    pub fn get_index_block(&self, segment_id: String, key: &[u8]) -> Option<Arc<IndexBlock>> {
        let key = (1, segment_id, key.to_vec().into());
        let item = self.data.get(&key)?;
        Some(item.right().clone())
    }
}
