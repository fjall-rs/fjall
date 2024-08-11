// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use std::sync::{PoisonError, RwLock, RwLockWriteGuard};

type Shard<T> = RwLock<T>;

/// Defines a sharded structure
///
/// The sharded structure consists of N shards that can be independently locked
///
/// This reduces contention when working with multiple threads
pub struct Sharded<T> {
    shards: Vec<Shard<T>>,
}

impl<T> std::ops::Deref for Sharded<T> {
    type Target = Vec<Shard<T>>;

    fn deref(&self) -> &Self::Target {
        &self.shards
    }
}

impl<T> Sharded<T> {
    /// Creates a new sharded structure
    pub fn new(shards: Vec<Shard<T>>) -> Self {
        Self { shards }
    }

    /// Gives write access to a shard
    pub fn write_one(&self) -> RwLockWriteGuard<'_, T> {
        loop {
            for shard in &self.shards {
                if let Ok(shard) = shard.try_write() {
                    return shard;
                }
            }
        }
    }

    /// Gives exclusive control over the entire structure
    pub fn full_lock(
        &self,
    ) -> Result<Vec<RwLockWriteGuard<'_, T>>, PoisonError<RwLockWriteGuard<'_, T>>> {
        self.shards.iter().map(|shard| shard.write()).collect()
    }
}
