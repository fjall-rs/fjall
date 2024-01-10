use crate::{batch::PartitionKey, PartitionHandle};
use lsm_tree::MemTable;
use std::{collections::HashMap, sync::Arc};

pub struct Task {
    /// ID of memtable
    pub(crate) id: Arc<str>,

    /// Memtable to flush
    pub(crate) sealed_memtable: Arc<MemTable>,

    /// Partition
    pub(crate) partition: PartitionHandle,
}

impl std::fmt::Debug for Task {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FlushTask {}:{}", self.partition.name, self.id)
    }
}

/// The [`FlushManager`] stores a dictionary of queues, each queue
/// containing a list of flush tasks.
///
/// Each flush task references a sealed memtable and the given partition.
#[derive(Default)]
#[allow(clippy::module_name_repetitions)]
pub struct FlushManager {
    pub queues: HashMap<PartitionKey, Vec<Arc<Task>>>,
}

impl FlushManager {
    /// Returns the amount of bytes that are queued to be flushed
    pub fn queued_size(&self) -> u64 {
        self.queues
            .values()
            .map(|x| {
                x.iter()
                    .map(|x| x.sealed_memtable.size())
                    .map(u64::from)
                    .sum::<u64>()
            })
            .sum::<u64>()
    }

    pub fn remove_partition(&mut self, name: &str) {
        self.queues.remove(name);
    }

    pub fn enqueue_task(&mut self, partition_name: PartitionKey, task: Task) {
        log::debug!(
            "Enqueuing {partition_name}:{} for flushing ({} B)",
            task.id,
            task.sealed_memtable.size()
        );

        self.queues
            .entry(partition_name)
            .or_insert_with(|| Vec::with_capacity(10))
            .push(Arc::new(task));
    }

    /// Returns a list of tasks per partition.
    pub fn collect_tasks(&mut self, limit: usize) -> HashMap<PartitionKey, Vec<Arc<Task>>> {
        let mut collected: HashMap<_, Vec<_>> = HashMap::default();
        let mut cnt = 0;

        // NOTE: Returning multiple tasks per partition is fine and will
        // help with flushing very active partitions.
        //
        // Because we are flushing them atomically inside one batch,
        // we will never cover up a lower seqno of some other segment.
        'outer: for (partition_name, queue) in &self.queues {
            for item in queue {
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

    pub fn dequeue_tasks(&mut self, partition_name: PartitionKey, cnt: usize) {
        let queue = self
            .queues
            .entry(partition_name)
            .or_insert_with(|| Vec::with_capacity(10));

        for _ in 0..cnt {
            queue.remove(0);
        }
    }
}
