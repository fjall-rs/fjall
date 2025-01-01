// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::queue::FlushQueue;
use crate::{
    batch::PartitionKey, write_buffer_manager::SpaceTracker, HashMap, HashSet, PartitionHandle,
};
use lsm_tree::{Memtable, SegmentId};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

pub struct Task {
    /// ID of memtable
    pub id: SegmentId,

    /// Memtable to flush
    pub sealed_memtable: Arc<Memtable>,

    /// Partition
    pub partition: PartitionHandle,
}

impl std::fmt::Debug for Task {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FlushTask {}:{}", self.partition.name, self.id)
    }
}

/// The [`FlushTaskQueue`] stores a dictionary of queues, each queue
/// containing some flush tasks.
///
/// Each flush task references a sealed memtable and the given partition.
#[allow(clippy::module_name_repetitions)]
#[derive(Debug)]
pub struct FlushTaskQueues {
    queues: RwLock<HashMap<PartitionKey, FlushQueue>>,
    /// Keeps track of write buffer size
    buffer_size: SpaceTracker,
}

impl FlushTaskQueues {
    pub fn new() -> Self {
        Self {
            queues: RwLock::default(),
            buffer_size: SpaceTracker::new(),
        }
    }

    #[inline]
    #[track_caller]
    fn queues_read_lock(&self) -> RwLockReadGuard<'_, HashMap<Arc<str>, FlushQueue>> {
        self.queues.read().expect("lock is poisoned")
    }

    #[inline]
    #[track_caller]
    fn queues_write_lock(&self) -> RwLockWriteGuard<'_, HashMap<Arc<str>, FlushQueue>> {
        self.queues.write().expect("lock is poisoned")
    }

    pub fn buffer_size(&self) -> u64 {
        self.buffer_size.get()
    }

    pub fn grow_buffer(&self, n: u64) -> u64 {
        self.buffer_size.allocate(n)
    }

    pub fn shrink_buffer(&self, n: u64) -> u64 {
        self.buffer_size.free(n)
    }

    #[track_caller]
    pub fn clear(&self) {
        self.queues_write_lock().clear();
    }

    /// Gets the names of partitions that have queued tasks.
    #[track_caller]
    pub fn get_partitions_with_tasks(&self) -> HashSet<PartitionKey> {
        self.queues_read_lock()
            .iter()
            .filter(|(_, v)| !v.is_empty())
            .map(|(k, _)| k)
            .cloned()
            .collect()
    }

    /// Returns the amount of queues.
    #[track_caller]
    pub fn queue_count(&self) -> usize {
        self.queues_read_lock().len()
    }

    /// Returns the amount of bytes queued.
    #[track_caller]
    pub fn queued_size(&self) -> u64 {
        self.queues_read_lock()
            .values()
            .map(FlushQueue::size)
            .sum::<u64>()
    }

    // NOTE: is actually used in tests
    #[allow(dead_code)]
    /// Returns the amount of tasks that are queued to be flushed.
    #[track_caller]
    pub fn task_count(&self) -> usize {
        self.queues_read_lock()
            .values()
            .map(FlushQueue::len)
            .sum::<usize>()
    }

    // NOTE: is actually used in tests
    #[allow(dead_code)]
    #[must_use]
    #[track_caller]
    pub fn is_empty(&self) -> bool {
        self.queues_read_lock().values().all(FlushQueue::is_empty)
    }

    #[track_caller]
    pub fn remove_partition(&self, name: &str) {
        self.queues_write_lock().remove(name);
    }

    #[track_caller]
    pub fn enqueue(&self, task: Task) {
        let partition_name = task.partition.name.clone();
        log::debug!(
            "Enqueuing {partition_name}:{} for flushing ({} B)",
            task.id,
            task.sealed_memtable.size()
        );

        self.queues_write_lock()
            .entry(partition_name)
            .or_default()
            .enqueue(Arc::new(task));
    }

    pub fn enqueue_recovery_tasks(&self, recovery_tasks: impl Iterator<Item = Task>) -> usize {
        let mut queues = self.queues_write_lock();
        let mut count = 0;

        for task in recovery_tasks {
            // IMPORTANT: Add sealed memtable size to current write buffer size
            self.grow_buffer(task.sealed_memtable.size().into());

            // TODO: unit test write buffer size after recovery

            // IMPORTANT: Add sealed memtable to flush manager, so it can be flushed
            let partition_name = task.partition.name.clone();

            log::debug!(
                "Enqueuing {partition_name}:{} for flushing ({} B)",
                task.id,
                task.sealed_memtable.size()
            );

            queues
                .entry(partition_name)
                .or_default()
                .enqueue(Arc::new(task));

            count += 1;
        }

        count
    }

    /// Returns a list of tasks per partition.
    #[track_caller]
    pub fn collect_tasks(&self, limit: usize) -> HashMap<PartitionKey, Vec<Arc<Task>>> {
        let mut collected: HashMap<_, Vec<_>> = HashMap::default();
        let mut cnt = 0;

        // NOTE: Returning multiple tasks per partition is fine and will
        // help with flushing very active partitions.
        //
        // Because we are flushing them atomically inside one batch,
        // we will never cover up a lower seqno of some other segment.
        // For this to work, all tasks need to be successful and atomically
        // applied (all-or-nothing).
        'outer: for (partition_name, queue) in self.queues_read_lock().iter() {
            for item in queue.iter() {
                if cnt == limit {
                    break 'outer;
                }

                collected
                    .entry(partition_name.clone())
                    .or_default()
                    .push(item.clone());

                cnt += 1;
            }
        }

        collected
    }

    #[track_caller]
    pub fn dequeue(&self, partition_name: PartitionKey, cnt: usize) {
        self.queues_write_lock()
            .entry(partition_name)
            .or_default()
            .dequeue(cnt);
    }
}
