// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    batch::PartitionKey,
    flush::manager::{FlushTaskQueue, Task},
    journal::{
        manager::{EvictionWatermark, Item, JournalItemQueue},
        writer::Writer,
    },
    keyspace::Partitions,
    recovery::recover_sealed_memtables,
};
use lsm_tree::SequenceNumberCounter;
use std::{
    path::PathBuf,
    sync::{Arc, MutexGuard, RwLock},
};

/// Keeps track of and flushes sealed journals and memtables
#[allow(clippy::module_name_repetitions)]
#[derive(Debug)]
pub struct FlushTracker {
    /// It caps the write buffer size by flushing memtables to disk segments.
    tasks: FlushTaskQueue,
    /// It checks on-disk journal size and flushes memtables if needed, to
    /// garbage collect sealed journals.
    items: JournalItemQueue,
}

impl Drop for FlushTracker {
    fn drop(&mut self) {
        log::trace!("Dropping flush tracker");

        #[cfg(feature = "__internal_whitebox")]
        crate::drop::decrement_drop_counter();
    }
}

impl FlushTracker {
    pub fn new(active_path: PathBuf) -> FlushTracker {
        #[cfg(feature = "__internal_whitebox")]
        crate::drop::increment_drop_counter();

        Self {
            tasks: FlushTaskQueue::new(),
            items: JournalItemQueue::from_active(active_path),
        }
    }

    #[allow(clippy::too_many_lines)]
    pub fn recover_sealed_memtables(
        &self,
        partitions: &RwLock<Partitions>,
        seqno: &SequenceNumberCounter,
        sealed_journal_paths: impl Iterator<Item = PathBuf>,
    ) -> crate::Result<()> {
        recover_sealed_memtables(self, partitions, seqno, sealed_journal_paths)
    }
}

/// Flush Task Queues
impl FlushTracker {
    pub fn clear_queues(&self) {
        self.tasks.clear();
    }

    /// Gets the names of partitions that have queued tasks.
    pub fn get_partitions_with_tasks(&self) -> Vec<PartitionKey> {
        self.tasks.get_partitions_with_tasks()
    }

    /// Returns the amount of queues.
    pub fn queue_count(&self) -> usize {
        self.tasks.queue_count()
    }

    /// Returns the amount of bytes queued.
    pub fn queued_size(&self) -> u64 {
        self.tasks.queued_size()
    }

    // NOTE: is actually used in tests
    #[allow(dead_code)]
    /// Returns the amount of tasks that are queued to be flushed.
    pub fn task_count(&self) -> usize {
        self.tasks.task_count()
    }

    // NOTE: is actually used in tests
    #[allow(dead_code)]
    #[must_use]
    pub fn is_task_queue_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    pub fn remove_partition(&self, name: &str) {
        self.tasks.remove_partition(name);
    }

    pub fn enqueue_task(&self, task: Task) {
        self.tasks.enqueue(task);
    }

    /// Returns a list of tasks per partition.
    pub fn collect_tasks(&self, limit: usize) -> Vec<Vec<Arc<Task>>> {
        self.tasks.collect_tasks(limit)
    }

    pub fn dequeue_tasks(&self, partition_name: &PartitionKey, cnt: usize) {
        self.tasks.dequeue(partition_name, cnt);
    }
}

/// Journal Item Queue
impl FlushTracker {
    pub fn clear_items(&self) {
        self.items.clear();
    }

    pub fn enqueue_item(&self, item: Item) {
        self.items.enqueue(item);
    }

    /// Returns the amount of journals
    pub fn journal_count(&self) -> usize {
        self.items.journal_count()
    }

    // NOTE: is actually used in tests
    #[allow(dead_code)]
    /// Returns the amount of sealed journals
    pub fn sealed_journal_count(&self) -> usize {
        self.items.sealed_journal_count()
    }

    /// Returns the amount of bytes used on disk by journals
    pub fn disk_space_used(&self) -> u64 {
        self.items.disk_space().get()
    }

    /// Performs maintenance, maybe deleting some old journals
    pub fn maintenance(&self) -> crate::Result<()> {
        self.items.maintenance()
    }

    pub fn rotate_journal(
        &self,
        journal_writer: &mut MutexGuard<Writer>,
        watermarks: Vec<EvictionWatermark>,
    ) -> crate::Result<()> {
        self.items.rotate_journal(journal_writer, watermarks)
    }
}

/// Write Buffer
impl FlushTracker {
    pub fn buffer_size(&self) -> u64 {
        self.tasks.buffer_size().get()
    }

    pub fn increment_buffer_size(&self, n: u64) -> u64 {
        self.tasks.buffer_size().increment(n)
    }

    pub fn decrement_buffer_size(&self, n: u64) -> u64 {
        self.tasks.buffer_size().decrement(n)
    }
}
