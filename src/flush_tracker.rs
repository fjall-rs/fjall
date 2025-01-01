use std::{
    path::PathBuf,
    sync::{Arc, MutexGuard},
};

use crate::{
    batch::PartitionKey,
    flush::manager::{FlushTaskQueues, Task},
    journal::{
        manager::{EvictionWatermark, Item, JournalItemQueue},
        writer::Writer,
    },
    HashMap, HashSet,
};

/// Keeps track of and flushes sealed journals and memtables
#[allow(clippy::module_name_repetitions)]
#[derive(Debug)]
pub struct FlushTracker {
    /// It caps the write buffer size by flushing memtables to disk segments.
    task_queues: FlushTaskQueues,
    /// It checks on-disk journal size and flushes memtables if needed, to
    /// garbage collect sealed journals.
    item_queue: JournalItemQueue,
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
            task_queues: FlushTaskQueues::new(),
            item_queue: JournalItemQueue::from_active(active_path),
        }
    }
}

/// Flush Task Queues
impl FlushTracker {
    #[inline(always)]
    pub fn clear_queues(&self) {
        self.task_queues.clear();
    }

    /// Gets the names of partitions that have queued tasks.
    #[inline(always)]
    pub fn get_partitions_with_tasks(&self) -> HashSet<PartitionKey> {
        self.task_queues.get_partitions_with_tasks()
    }

    /// Returns the amount of queues.
    #[inline(always)]
    pub fn queue_count(&self) -> usize {
        self.task_queues.queue_count()
    }

    /// Returns the amount of bytes queued.
    #[inline(always)]
    pub fn queued_size(&self) -> u64 {
        self.task_queues.queued_size()
    }

    // NOTE: is actually used in tests
    #[allow(dead_code)]
    /// Returns the amount of tasks that are queued to be flushed.
    #[inline(always)]
    pub fn task_count(&self) -> usize {
        self.task_queues.task_count()
    }

    // NOTE: is actually used in tests
    #[allow(dead_code)]
    #[must_use]
    #[inline(always)]
    pub fn is_task_queue_empty(&self) -> bool {
        self.task_queues.is_empty()
    }

    #[inline(always)]
    pub fn remove_partition(&self, name: &str) {
        self.task_queues.remove_partition(name);
    }

    #[inline(always)]
    pub fn enqueue_task(&self, task: Task) {
        self.task_queues.enqueue(task);
    }

    #[inline(always)]
    pub fn enqueue_recovery_tasks(&self, recovery_tasks: impl Iterator<Item = Task>) -> usize {
        self.task_queues.enqueue_recovery_tasks(recovery_tasks)
    }

    /// Returns a list of tasks per partition.
    #[inline(always)]
    pub fn collect_tasks(&self, limit: usize) -> HashMap<PartitionKey, Vec<Arc<Task>>> {
        self.task_queues.collect_tasks(limit)
    }

    #[inline(always)]
    pub fn dequeue_tasks(&self, partition_name: PartitionKey, cnt: usize) {
        self.task_queues.dequeue(partition_name, cnt);
    }
}

/// Journal Item Queue
impl FlushTracker {
    #[inline(always)]
    pub fn clear_items(&self) {
        self.item_queue.clear();
    }

    #[inline(always)]
    pub fn enqueue_item(&self, item: Item) {
        self.item_queue.enqueue(item);
    }

    /// Returns the amount of journals
    #[inline(always)]
    pub fn journal_count(&self) -> usize {
        self.item_queue.journal_count()
    }

    // NOTE: is actually used in tests
    #[allow(dead_code)]
    /// Returns the amount of sealed journals
    #[inline(always)]
    pub fn sealed_journal_count(&self) -> usize {
        self.item_queue.sealed_journal_count()
    }

    /// Returns the amount of bytes used on disk by journals
    pub fn disk_space_used(&self) -> u64 {
        self.item_queue.disk_space_used()
    }

    /// Performs maintenance, maybe deleting some old journals
    #[inline(always)]
    pub fn maintenance(&self) -> crate::Result<()> {
        self.item_queue.maintenance()
    }

    #[inline(always)]
    pub fn rotate_journal(
        &self,
        journal_writer: &mut MutexGuard<Writer>,
        watermarks: Vec<EvictionWatermark>,
    ) -> crate::Result<()> {
        self.item_queue.rotate_journal(journal_writer, watermarks)
    }
}

/// Write Buffer
impl FlushTracker {
    #[inline(always)]
    pub fn buffer_size(&self) -> u64 {
        self.task_queues.buffer_size()
    }

    #[inline(always)]
    pub fn grow_buffer(&self, n: u64) -> u64 {
        self.task_queues.grow_buffer(n)
    }

    #[inline(always)]
    pub fn shrink_buffer(&self, n: u64) -> u64 {
        self.task_queues.shrink_buffer(n)
    }
}
