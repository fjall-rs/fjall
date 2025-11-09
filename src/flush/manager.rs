// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::queue::FlushQueue;
use crate::{keyspace::InternalKeyspaceId, HashMap, HashSet, Keyspace};
use lsm_tree::{Memtable, TableId};
use std::sync::Arc;

pub struct Task {
    /// ID of memtable
    pub(crate) id: TableId,

    /// Memtable to flush
    pub(crate) sealed_memtable: Arc<Memtable>,

    /// Keyspace
    pub(crate) keyspace: Keyspace,
}

impl std::fmt::Debug for Task {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FlushTask {}:{}", self.keyspace.name, self.id)
    }
}

// TODO: accessing flush manager shouldn't take RwLock... but changing its internals should

/// The [`FlushManager`] stores a dictionary of queues, each queue
/// containing some flush tasks.
///
/// Each flush task references a sealed memtable and the given keyspace.
#[allow(clippy::module_name_repetitions)]
#[derive(Debug)]
pub struct FlushManager {
    queues: HashMap<InternalKeyspaceId, FlushQueue>,
}

impl Drop for FlushManager {
    fn drop(&mut self) {
        log::trace!("Dropping flush manager");

        #[cfg(feature = "__internal_whitebox")]
        crate::drop::decrement_drop_counter();
    }
}

impl FlushManager {
    pub(crate) fn new() -> Self {
        #[cfg(feature = "__internal_whitebox")]
        crate::drop::increment_drop_counter();

        Self {
            queues: HashMap::default(),
        }
    }

    pub(crate) fn clear(&mut self) {
        self.queues.clear();
    }

    /// Gets the names of keyspaces that have queued tasks.
    pub(crate) fn get_keyspaces_with_tasks(&self) -> HashSet<InternalKeyspaceId> {
        self.queues
            .iter()
            .filter(|(_, v)| !v.is_empty())
            .map(|(k, _)| k)
            .cloned()
            .collect()
    }

    /// Returns the number of queues.
    pub(crate) fn queue_count(&self) -> usize {
        self.queues.len()
    }

    /// Returns the amount of bytes queued.
    pub(crate) fn queued_size(&self) -> u64 {
        self.queues.values().map(FlushQueue::size).sum::<u64>()
    }

    // NOTE: is actually used in tests
    #[allow(dead_code)]
    /// Returns the number of tasks that are queued to be flushed.
    pub(crate) fn len(&self) -> usize {
        self.queues.values().map(FlushQueue::len).sum::<usize>()
    }

    // NOTE: is actually used in tests
    #[allow(dead_code)]
    #[must_use]
    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(crate) fn remove_keyspace(&mut self, id: InternalKeyspaceId) {
        self.queues.remove(&id);
    }

    pub(crate) fn enqueue_task(&mut self, keyspace_id: InternalKeyspaceId, task: Task) {
        log::debug!(
            "Enqueuing {keyspace_id}:{} for flushing ({} B)",
            task.id,
            task.sealed_memtable.size()
        );

        self.queues
            .entry(keyspace_id)
            .or_default()
            .enqueue(Arc::new(task));
    }

    /// Returns a list of tasks per keyspace.
    pub(crate) fn collect_tasks(
        &mut self,
        limit: usize,
    ) -> HashMap<InternalKeyspaceId, Vec<Arc<Task>>> {
        let mut collected: HashMap<_, Vec<_>> = HashMap::default();
        let mut cnt = 0;

        // NOTE: Returning multiple tasks per keyspace is fine and will
        // help with flushing very active keyspaces.
        //
        // Because we are flushing them atomically inside one batch,
        // we will never cover up a lower seqno of some other table.
        // For this to work, all tasks need to be successful and atomically
        // applied (all-or-nothing).
        'outer: for (keyspace_id, queue) in &self.queues {
            for item in queue.iter() {
                if cnt == limit {
                    break 'outer;
                }

                collected
                    .entry(*keyspace_id)
                    .or_default()
                    .push(item.clone());

                cnt += 1;
            }
        }

        collected
    }

    pub(crate) fn dequeue_tasks(&mut self, keyspace_id: InternalKeyspaceId, cnt: usize) {
        self.queues.entry(keyspace_id).or_default().dequeue(cnt);
    }
}
