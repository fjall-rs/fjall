// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::Keyspace;
use std::{collections::VecDeque, sync::Mutex};
use std_semaphore::Semaphore;

// TODO: 3.0.0 use flume instead

/// The compaction manager keeps track of which keyspaces
/// have recently been flushed in a FIFO queue.
///
/// Its semaphore notifies compaction threads which will wake
/// up and consume the queue items.
///
/// The semaphore is incremented by the flush worker and optionally
/// by the individual keyspaces in case of write halting.
#[allow(clippy::module_name_repetitions)]
pub struct CompactionManager {
    keyspaces: Mutex<VecDeque<Keyspace>>,
    semaphore: Semaphore,
}

impl Drop for CompactionManager {
    fn drop(&mut self) {
        log::trace!("Dropping compaction manager");
    }
}

impl Default for CompactionManager {
    fn default() -> Self {
        Self {
            keyspaces: Mutex::new(VecDeque::with_capacity(10)),
            semaphore: Semaphore::new(0),
        }
    }
}

impl CompactionManager {
    pub fn len(&self) -> usize {
        self.keyspaces.lock().expect("lock is poisoned").len()
    }

    pub fn clear(&self) {
        self.keyspaces.lock().expect("lock is poisoned").clear();
    }

    pub fn remove_keyspace(&self, name: &str) {
        let mut lock = self.keyspaces.lock().expect("lock is poisoned");
        lock.retain(|x| &*x.name != name);
    }

    pub fn wait_for(&self) {
        self.semaphore.acquire();
    }

    pub fn notify(&self, keyspace: Keyspace) {
        let mut lock = self.keyspaces.lock().expect("lock is poisoned");
        lock.push_back(keyspace);
        self.semaphore.release();
    }

    pub fn notify_empty(&self) {
        self.semaphore.release();
    }

    pub fn pop(&self) -> Option<Keyspace> {
        let mut lock = self.keyspaces.lock().expect("lock is poisoned");
        lock.pop_front()
    }
}
