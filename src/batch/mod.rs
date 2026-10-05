// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

pub mod item;

use crate::write_pipeline::Batch as WritePipelineBatch;
use crate::{Database, Keyspace, PersistMode};
use item::Item;
use lsm_tree::{UserKey, UserValue, ValueType};
use std::sync::Arc;

/// An atomic write batch
///
/// Allows atomically writing across keyspaces inside the [`Database`].
pub struct WriteBatch {
    pub(crate) data: Vec<Item>,
    db: Database,
    durability: Option<PersistMode>,
}

impl WriteBatch {
    /// Initializes a new write batch.
    ///
    /// This function is called by [`Database::batch`].
    pub(crate) fn new(db: Database) -> Self {
        Self {
            data: Vec::new(),
            db,
            durability: None,
        }
    }

    /// Initializes a new write batch with preallocated capacity.
    ///
    /// ### Note
    ///
    /// "Capacity" refers to the number of batch item slots, not their size in memory.
    #[must_use]
    pub fn with_capacity(db: Database, capacity: usize) -> Self {
        Self {
            data: Vec::with_capacity(capacity),
            db,
            durability: None,
        }
    }

    /// Gets the number of batched items.
    #[must_use]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Returns `true` if there are no batches items (yet).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Sets the durability level.
    #[must_use]
    pub fn durability(mut self, mode: Option<PersistMode>) -> Self {
        self.durability = mode;
        self
    }

    /// Inserts a key-value pair into the batch.
    pub fn insert<K: Into<UserKey>, V: Into<UserValue>>(&mut self, p: &Keyspace, key: K, value: V) {
        self.data.push(Item::new(
            p.clone(),
            key.into(),
            value.into(),
            ValueType::Value,
        ));
    }

    /// Removes a key-value pair.
    pub fn remove<K: Into<UserKey>>(&mut self, p: &Keyspace, key: K) {
        self.data.push(Item::new(
            p.clone(),
            key.into(),
            UserValue::default(),
            ValueType::Tombstone,
        ));
    }

    /// Adds a weak tombstone marker for a key.
    ///
    /// The tombstone marker of this delete operation will vanish when it
    /// collides with its corresponding insertion.
    /// This may cause older versions of the value to be resurrected, so it should
    /// only be used and preferred in scenarios where a key is only ever written once.
    ///
    /// # Experimental
    ///
    /// This function is currently experimental.
    #[doc(hidden)]
    pub fn remove_weak<K: Into<UserKey>>(&mut self, p: &Keyspace, key: K) {
        self.data.push(Item::new(
            p.clone(),
            key.into(),
            UserValue::default(),
            ValueType::WeakTombstone,
        ));
    }

    /// Commits the batch to the [`Database`] atomically.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    #[allow(clippy::missing_panics_doc)]
    pub fn commit(self) -> crate::Result<()> {
        if self.is_empty() {
            return Ok(());
        }

        let write_record =
            crate::write_pipeline::WriteRecord::new(WritePipelineBatch::Batch(self.data))
                .persist_mode(PersistMode::with_manual_flag(
                    self.durability,
                    self.db.config.manual_journal_persist,
                ));

        self.db
            .supervisor
            .write_pipeline
            .commit(Arc::new(write_record))?;

        Ok(())
    }
}
