use crate::PartitionHandle;
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
    pub queues: HashMap<Arc<str>, Vec<Arc<Task>>>,
}

impl FlushManager {
    pub fn enqueue_task(&mut self, partition_name: Arc<str>, task: Task) {
        log::debug!("Enqueuing {partition_name}:{} for flushing", task.id);

        self.queues
            .entry(partition_name)
            .or_insert_with(|| Vec::with_capacity(10))
            .push(Arc::new(task));
    }

    /// Returns a list of tasks per partition.
    pub fn collect_tasks(&mut self, limit: usize) -> HashMap<Arc<str>, Vec<Arc<Task>>> {
        let mut collected: HashMap<Arc<str>, Vec<Arc<Task>>> = HashMap::new();
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

    pub fn dequeue_task(&mut self, partition_name: Arc<str>) {
        self.queues
            .entry(partition_name)
            .or_insert_with(|| Vec::with_capacity(10))
            .remove(0);
    }
}
