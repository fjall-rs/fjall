// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::keyspace::InternalKeyspaceId;
use lsm_tree::{UserKey, UserValue, ValueType};

#[derive(Clone, PartialEq, Eq)]
pub struct Item {
    /// Internal keyspace ID
    pub keyspace_id: InternalKeyspaceId,

    /// User-defined key - an arbitrary byte array
    ///
    /// Supports up to 2^16 bytes
    pub key: UserKey,

    /// User-defined value - an arbitrary byte array
    ///
    /// Supports up to 65535 bytes
    pub value: UserValue,

    /// Tombstone marker - if this is true, the value has been deleted
    pub value_type: ValueType,
}

impl std::fmt::Debug for Item {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}:{:?}:{} => {:?}",
            self.keyspace_id,
            self.key,
            match self.value_type {
                ValueType::Value => "V",
                ValueType::Tombstone => "T",
                ValueType::WeakTombstone => "W",
                ValueType::Indirection => "Vb",
            },
            self.value
        )
    }
}

impl Item {
    pub fn new<K: Into<UserKey>, V: Into<UserValue>>(
        keyspace_id: InternalKeyspaceId,
        key: K,
        value: V,
        value_type: ValueType,
    ) -> Self {
        let k = key.into();
        let v = value.into();

        assert!(!k.is_empty());

        assert!(
            u16::try_from(k.len()).is_ok(),
            "Keys can be up to 65535 bytes long"
        );
        assert!(
            u32::try_from(v.len()).is_ok(),
            "Values can be up to 2^32 bytes long"
        );

        Self {
            keyspace_id,
            key: k,
            value: v,
            value_type,
        }
    }
}
