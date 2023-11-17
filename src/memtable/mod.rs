mod recovery;

use crate::Value;
use std::collections::BTreeMap;

/// The `MemTable` serves as an intermediary storage for new items
///
/// If the memtable's size exceeds a certain threshold, it will be written to disk as a Segment and cleared
///
/// In case of a program crash, the current memtable can be rebuilt from the commit log
#[derive(Default)]
pub struct MemTable {
    pub(crate) data: BTreeMap<Vec<u8>, Value>,
    //size_in_bytes: u64,
}

impl MemTable {
    /// Returns the item by key if it exists
    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Option<Value> {
        let result = self.data.get(key.as_ref());
        result.cloned()
    }

    /*   #[allow(dead_code)]
    pub(crate) fn get_size(&self) -> u64 {
        self.size_in_bytes
    } */

    /* #[allow(dead_code)]
    pub(crate) fn set_size(&mut self, value: u64) {
        self.size_in_bytes = value;
    } */

    pub fn insert(&mut self, entry: Value, bytes_written: usize) {
        self.data.insert(entry.key.clone(), entry);
        //self.size_in_bytes += bytes_written as u64;
    }

    /* pub fn exceeds_threshold(&mut self, threshold: u64) -> bool {
        self.size_in_bytes > threshold
    } */
}
