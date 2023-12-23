use std::sync::{Mutex, MutexGuard, PoisonError};

type Shard<T> = Mutex<T>;

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
    pub fn lock_one(&self) -> MutexGuard<'_, T> {
        loop {
            for shard in &self.shards {
                if let Ok(shard) = shard.try_lock() {
                    return shard;
                }
            }
        }
    }

    /// Gives exclusive control over the entire structure
    pub fn full_lock(&self) -> Result<Vec<MutexGuard<'_, T>>, PoisonError<MutexGuard<'_, T>>> {
        self.shards.iter().map(|shard| shard.lock()).collect()
    }
}