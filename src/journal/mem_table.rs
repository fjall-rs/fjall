use crate::{
    value::{ParsedInternalKey, SeqNo, UserData},
    Value,
};
use std::collections::BTreeMap;

#[derive(Default)]
pub struct MemTable {
    pub(crate) items: BTreeMap<ParsedInternalKey, UserData>,
    pub(crate) size_in_bytes: u32,
}

impl MemTable {
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Returns the item by key if it exists
    ///
    /// The item with the highest seqno will be returned
    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Option<Value> {
        let prefix = key.as_ref();

        // NOTE: This range start deserves some explanation...
        // InternalKeys are multi-sorted by 2 categories: user_key and Reverse(seqno). (tombstone doesn't really matter)
        // We search for the lowest entry that is greater or equal the user's prefix key
        // and has the highest seqno (because the seqno is stored in reverse order)
        //
        // Example: We search for "asd"
        //
        // key -> seqno
        //
        // a   -> 7
        // abc -> 5 <<< This is the lowest key that matches the range
        // abc -> 4
        // abc -> 3
        // abcdef -> 6
        // abcdef -> 5
        //
        let range = ParsedInternalKey::new(&key, SeqNo::MAX, true)..;

        let item = self
            .items
            .range(range)
            .find(|(key, _)| key.user_key.starts_with(prefix));

        item.map(|(key, value)| (key.clone(), value.clone()))
            .map(Value::from)
    }

    /// Inserts an item into the `MemTable`
    pub fn insert(&mut self, entry: Value) {
        let key = ParsedInternalKey::new(entry.key, entry.seqno, entry.is_tombstone);
        let value = entry.value;

        self.items.insert(key, value);
    }
}
