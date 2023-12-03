use crate::value::{ParsedInternalKey, SeqNo, UserData};
use crate::Value;
use crossbeam_skiplist::SkipMap;

/// The `MemTable` serves as an intermediary storage for new items
///
/// If the `MemTable`'s size exceeds a certain threshold, it will be written to disk as a Segment and cleared
///
/// In case of a program crash, the current `MemTable` can be rebuilt from the commit log
#[derive(Default)]
pub struct MemTable {
    pub(crate) items: SkipMap<ParsedInternalKey, UserData>,
}

impl MemTable {
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
        let range = ParsedInternalKey::new(key.as_ref().to_vec(), SeqNo::MAX, true)..;

        let item = self
            .items
            .range(range)
            .find(|entry| entry.key().user_key.starts_with(prefix));

        item.map(|entry| (entry.key().clone(), entry.value().clone()))
            .map(Value::from)
    }

    /// Inserts an item into the `MemTable`
    pub fn insert(&self, entry: Value) {
        let key = ParsedInternalKey::new(entry.key, entry.seqno, entry.is_tombstone);
        self.items.insert(key, entry.value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_log::test;

    #[test]
    fn test_memtable_get() {
        let mut memtable = MemTable::default();

        let value = Value::new("abc", "abc", false, 0);

        memtable.insert(value.clone());

        assert_eq!(Some(value), memtable.get("abc"));
    }

    #[test]
    fn test_memtable_get_highest_seqno() {
        let mut memtable = MemTable::default();

        memtable.insert(Value::new("abc", "abc", false, 0));
        memtable.insert(Value::new("abc", "abc", false, 1));
        memtable.insert(Value::new("abc", "abc", false, 2));
        memtable.insert(Value::new("abc", "abc", false, 3));
        memtable.insert(Value::new("abc", "abc", false, 4));

        assert_eq!(
            Some(Value::new("abc", "abc", false, 4)),
            memtable.get("abc")
        );
    }

    #[test]
    fn test_memtable_get_prefix() {
        let mut memtable = MemTable::default();

        memtable.insert(Value::new("abc0", "abc", false, 0));
        memtable.insert(Value::new("abc", "abc", false, 255));

        assert_eq!(
            Some(Value::new("abc", "abc", false, 255)),
            memtable.get("abc")
        );

        assert_eq!(
            Some(Value::new("abc0", "abc", false, 0)),
            memtable.get("abc0")
        );
    }
}
