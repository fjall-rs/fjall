use super::PartitionKey;
use lsm_tree::{UserKey, UserValue, ValueType};

pub struct Item {
    /// Partition key - an arbitrary byte array
    ///
    /// Supports up to 2^8 bytes
    pub partition: PartitionKey,

    /// User-defined key - an arbitrary byte array
    ///
    /// Supports up to 2^16 bytes
    pub key: UserKey,

    /// User-defined value - an arbitrary byte array
    ///
    /// Supports up to 2^16 bytes
    pub value: UserValue,

    /// Tombstone marker - if this is true, the value has been deleted
    pub value_type: ValueType,
}

impl Item {
    pub fn new<P: Into<PartitionKey>, K: Into<UserKey>, V: Into<UserValue>>(
        partition: P,
        key: K,
        value: V,
        value_type: ValueType,
    ) -> Self {
        let p = partition.into();
        let k = key.into();
        let v = value.into();

        assert!(!p.is_empty());
        assert!(!k.is_empty());
        assert!(p.len() <= u8::MAX.into());
        assert!(k.len() <= u16::MAX.into());
        assert!(v.len() <= u16::MAX.into());

        Self {
            partition: p,
            key: k,
            value: v,
            value_type,
        }
    }
}