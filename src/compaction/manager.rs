// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::PartitionHandle;
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};
use std_semaphore::Semaphore;

pub struct CompactionManagerInner {
    partitions: Mutex<VecDeque<PartitionHandle>>,
    semaphore: Semaphore,
}

impl Drop for CompactionManagerInner {
    fn drop(&mut self) {
        log::trace!("Dropping compaction manager");
    }
}

impl Default for CompactionManagerInner {
    fn default() -> Self {
        Self {
            partitions: Mutex::new(VecDeque::with_capacity(10)),
            semaphore: Semaphore::new(0),
        }
    }
}

/// The compaction manager keeps track of which partitions
/// have recently been flushed in a FIFO queue.
///
/// Its semaphore notifies compaction threads which will wake
/// up and consume the queue items.
///
/// The semaphore is incremented by the flush worker and optionally
/// by the individual partitions in case of write halting.
#[derive(Clone, Default)]
#[allow(clippy::module_name_repetitions)]
pub struct CompactionManager(Arc<CompactionManagerInner>);

impl std::ops::Deref for CompactionManager {
    type Target = CompactionManagerInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl CompactionManager {
    pub fn clear(&self) {
        self.partitions.lock().expect("lock is poisoned").clear();
    }

    pub fn remove_partition(&self, name: &str) {
        let mut lock = self.partitions.lock().expect("lock is poisoned");
        lock.retain(|x| &*x.name != name);
    }

    pub fn wait_for(&self) {
        self.semaphore.acquire();
    }

    pub fn notify(&self, partition: PartitionHandle) {
        let mut lock = self.partitions.lock().expect("lock is poisoned");
        lock.push_back(partition);
        self.semaphore.release();
    }

    pub fn notify_empty(&self) {
        self.semaphore.release();
    }

    pub fn pop(&self) -> Option<PartitionHandle> {
        let mut lock = self.partitions.lock().expect("lock is poisoned");
        lock.pop_front()
    }
}
