// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use std::sync::{atomic::AtomicU64, Arc};

/// Keeps track of the size of the keyspace's write buffer
#[derive(Clone, Default, Debug)]
pub struct WriteBufferManager(Arc<AtomicU64>);

impl std::ops::Deref for WriteBufferManager {
    type Target = AtomicU64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl WriteBufferManager {
    pub fn get(&self) -> u64 {
        self.load(std::sync::atomic::Ordering::Acquire)
    }

    // Adds some bytes to the write buffer counter.
    //
    // Returns the counter *after* incrementing.
    pub fn allocate(&self, n: u64) -> u64 {
        let before = self.fetch_add(n, std::sync::atomic::Ordering::AcqRel);
        before + n
    }

    // Frees some bytes from the write buffer counter.
    //
    // Returns the counter *after* decrementing.
    pub fn free(&self, n: u64) -> u64 {
        use std::sync::atomic::Ordering::{Acquire, SeqCst};

        loop {
            let now = self.load(Acquire);
            let subbed = now.saturating_sub(n);

            if self.compare_exchange(now, subbed, SeqCst, SeqCst).is_ok() {
                return subbed;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_log::test;

    #[test]
    fn write_buffer_manager_increment() {
        let m = WriteBufferManager::default();
        m.allocate(5);
        assert_eq!(m.get(), 5);

        m.allocate(15);
        assert_eq!(m.get(), 20);
    }

    #[test]
    fn write_buffer_manager_decrement() {
        let m = WriteBufferManager::default();
        m.allocate(20);
        assert_eq!(m.get(), 20);

        m.free(5);
        assert_eq!(m.get(), 15);

        m.free(20);
        assert_eq!(m.get(), 0);
    }
}
