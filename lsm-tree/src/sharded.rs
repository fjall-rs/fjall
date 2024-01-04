use std::sync::{Mutex, MutexGuard};

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

impl<T> std::ops::DerefMut for Sharded<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.shards
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
}
