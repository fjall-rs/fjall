use crate::value::{ParsedInternalKey, SeqNo, UserData};
use crate::Value;
use crossbeam_skiplist::SkipMap;

/// The `MemTable` serves as an intermediary storage for new items
///
/// If the `MemTable`'s size exceeds a certain threshold, it will be written to disk as a Segment and cleared
///
/// In case of a program crash, the current `MemTable` can be rebuilt from the last active journal
#[derive(Default)]
pub struct MemTable {
    pub(crate) items: SkipMap<ParsedInternalKey, UserData>,
}

impl MemTable {
    /// Returns the item by key if it exists
    ///
    /// The item with the highest seqno will be returned
    pub fn get<K: AsRef<[u8]>>(&self, key: K, seqno: Option<SeqNo>) -> Option<Value> {
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
        let range = ParsedInternalKey::new(prefix, SeqNo::MAX, true)..;

        for entry in self.items.range(range) {
            let key = entry.key();

            // We are past the user key, so we can immediately return None
            if !key.user_key.starts_with(prefix) {
                return None;
            }

            // Check for seqno if needed
            if let Some(seqno) = seqno {
                if key.seqno < seqno {
                    return Some(Value::from((entry.key().clone(), entry.value().clone())));
                }
            } else {
                return Some(Value::from((entry.key().clone(), entry.value().clone())));
            }
        }

        None
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
        let memtable = MemTable::default();

        let value = Value::new(b"abc".to_vec(), b"abc".to_vec(), false, 0);

        memtable.insert(value.clone());

        assert_eq!(Some(value), memtable.get("abc", None));
    }

    #[test]
    fn test_memtable_get_highest_seqno() {
        let memtable = MemTable::default();

        memtable.insert(Value::new(b"abc".to_vec(), b"abc".to_vec(), false, 0));
        memtable.insert(Value::new(b"abc".to_vec(), b"abc".to_vec(), false, 1));
        memtable.insert(Value::new(b"abc".to_vec(), b"abc".to_vec(), false, 2));
        memtable.insert(Value::new(b"abc".to_vec(), b"abc".to_vec(), false, 3));
        memtable.insert(Value::new(b"abc".to_vec(), b"abc".to_vec(), false, 4));

        assert_eq!(
            Some(Value::new(b"abc".to_vec(), b"abc".to_vec(), false, 4)),
            memtable.get("abc", None)
        );
    }

    #[test]
    fn test_memtable_get_prefix() {
        let memtable = MemTable::default();

        memtable.insert(Value::new(b"abc0".to_vec(), b"abc".to_vec(), false, 0));
        memtable.insert(Value::new(b"abc".to_vec(), b"abc".to_vec(), false, 255));

        assert_eq!(
            Some(Value::new(b"abc".to_vec(), b"abc".to_vec(), false, 255)),
            memtable.get("abc", None)
        );

        assert_eq!(
            Some(Value::new(b"abc0".to_vec(), b"abc".to_vec(), false, 0)),
            memtable.get("abc0", None)
        );
    }

    #[test]
    fn test_memtable_get_old_version() {
        let memtable = MemTable::default();

        memtable.insert(Value::new(b"abc".to_vec(), b"abc".to_vec(), false, 0));
        memtable.insert(Value::new(b"abc".to_vec(), b"abc".to_vec(), false, 99));
        memtable.insert(Value::new(b"abc".to_vec(), b"abc".to_vec(), false, 255));

        assert_eq!(
            Some(Value::new(b"abc".to_vec(), b"abc".to_vec(), false, 255)),
            memtable.get("abc", None)
        );

        assert_eq!(
            Some(Value::new(b"abc".to_vec(), b"abc".to_vec(), false, 99)),
            memtable.get("abc", Some(100))
        );

        assert_eq!(
            Some(Value::new(b"abc".to_vec(), b"abc".to_vec(), false, 0)),
            memtable.get("abc", Some(50))
        );
    }
}
