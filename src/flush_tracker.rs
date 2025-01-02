use crate::{
    batch::PartitionKey,
    flush::manager::{FlushTaskQueues, Task},
    journal::{
        batch_reader::JournalBatchReader,
        manager::{EvictionWatermark, Item, JournalItemQueue},
        reader::JournalReader,
        writer::Writer,
    },
    keyspace::Partitions,
};
use lsm_tree::{AbstractTree, SequenceNumberCounter};
use std::{
    path::PathBuf,
    sync::{Arc, MutexGuard, RwLock},
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

    #[allow(clippy::too_many_lines)]
    #[track_caller]
    pub fn recover_sealed_memtables(
        &self,
        partitions: &RwLock<Partitions>,
        seqno: &SequenceNumberCounter,
        sealed_journal_paths: impl Iterator<Item = PathBuf>,
    ) -> crate::Result<()> {
        #[allow(clippy::significant_drop_tightening)]
        let partitions_lock = partitions.read().expect("lock is poisoned");

        for journal_path in sealed_journal_paths {
            log::debug!("Recovering sealed journal: {journal_path:?}");

            let journal_size = journal_path.metadata()?.len();

            log::debug!("Reading sealed journal at {journal_path:?}");

            let raw_reader = JournalReader::new(&journal_path)?;
            let reader = JournalBatchReader::new(raw_reader);

            let mut watermarks: Vec<EvictionWatermark> = Vec::new();

            for batch in reader {
                let batch = batch?;

                for item in batch.items {
                    if let Some(handle) = partitions_lock.get(&item.partition) {
                        let tree = &handle.tree;

                        match watermarks.binary_search_by(|watermark| {
                            watermark.partition.name.cmp(&item.partition)
                        }) {
                            Ok(index) => {
                                let prev = &mut watermarks[index];
                                prev.lsn = prev.lsn.max(batch.seqno);
                            }
                            Err(index) => {
                                watermarks.insert(
                                    index,
                                    EvictionWatermark {
                                        partition: handle.clone(),
                                        lsn: batch.seqno,
                                    },
                                );
                            }
                        };

                        match item.value_type {
                            lsm_tree::ValueType::Value => {
                                tree.insert(item.key, item.value, batch.seqno);
                            }
                            lsm_tree::ValueType::Tombstone => {
                                tree.remove(item.key, batch.seqno);
                            }
                            lsm_tree::ValueType::WeakTombstone => {
                                tree.remove_weak(item.key, batch.seqno);
                            }
                        }
                    }
                }
            }

            log::debug!("Sealing recovered memtables");
            let mut recovered_count = 0;

            for handle in &watermarks {
                let tree = &handle.partition.tree;

                let partition_lsn = tree.get_highest_persisted_seqno();

                // IMPORTANT: Only apply sealed memtables to partitions
                // that have a lower seqno to avoid double flushing
                let should_skip_sealed_memtable =
                    partition_lsn.map_or(false, |partition_lsn| partition_lsn >= handle.lsn);

                if should_skip_sealed_memtable {
                    handle.partition.tree.lock_active_memtable().clear();

                    log::trace!(
                        "Partition {} has higher seqno ({partition_lsn:?}), skipping",
                        handle.partition.name
                    );
                    continue;
                }

                if let Some((memtable_id, sealed_memtable)) = tree.rotate_memtable() {
                    assert_eq!(
                        Some(handle.lsn),
                        sealed_memtable.get_highest_seqno(),
                        "memtable lsn does not match what was recovered - this is a bug"
                    );

                    log::trace!(
                        "sealed memtable of {} has {} items",
                        handle.partition.name,
                        sealed_memtable.len(),
                    );

                    // Maybe the memtable has a higher seqno, so try to set to maximum
                    let maybe_next_seqno =
                        tree.get_highest_seqno().map(|x| x + 1).unwrap_or_default();

                    seqno.fetch_max(maybe_next_seqno, std::sync::atomic::Ordering::AcqRel);

                    log::debug!("Keyspace seqno is now {}", seqno.get());

                    // IMPORTANT: Add sealed memtable size to current write buffer size
                    self.task_queues
                        .buffer_size()
                        .allocate(sealed_memtable.size().into());

                    // TODO: unit test write buffer size after recovery

                    // IMPORTANT: Add sealed memtable to flush manager, so it can be flushed
                    self.task_queues.enqueue(Task {
                        id: memtable_id,
                        sealed_memtable,
                        partition: handle.partition.clone(),
                    });

                    recovered_count += 1;
                };
            }

            log::debug!("Recovered {recovered_count} sealed memtables");

            // IMPORTANT: Add sealed journal to journal manager
            self.item_queue.enqueue(Item {
                watermarks,
                path: journal_path.clone(),
                size_in_bytes: journal_size,
            });

            log::debug!("Requeued sealed journal at {:?}", journal_path);
        }

        Ok(())
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
    pub fn get_partitions_with_tasks(&self) -> Vec<PartitionKey> {
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

    /// Returns a list of tasks per partition.
    #[inline(always)]
    pub fn collect_tasks(&self, limit: usize) -> Vec<Vec<Arc<Task>>> {
        self.task_queues.collect_tasks(limit)
    }

    #[inline(always)]
    pub fn dequeue_tasks(&self, partition_name: &PartitionKey, cnt: usize) {
        self.task_queues.dequeue(partition_name, cnt);
    }
}

/// Journal Item Queue
impl FlushTracker {
    #[inline(always)]
    pub fn clear_items(&self) {
        self.item_queue.clear();
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
    #[inline(always)]
    pub fn disk_space_used(&self) -> u64 {
        self.item_queue.disk_space().get()
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
        self.task_queues.buffer_size().get()
    }

    #[inline(always)]
    pub fn grow_buffer(&self, n: u64) -> u64 {
        self.task_queues.buffer_size().allocate(n)
    }

    #[inline(always)]
    pub fn shrink_buffer(&self, n: u64) -> u64 {
        self.task_queues.buffer_size().free(n)
    }
}
