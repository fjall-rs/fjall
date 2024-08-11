// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::manager::Task;
use std::sync::Arc;

/// A FIFO queue of flush tasks.
///
/// Allows peeking N items into the queue head to allow
/// parallel processing of tasks of a single partition,
/// which allows processing N tasks even if there is just
/// one partition.
///
/// This only really works because there is one flush thread
/// that spawns flush workers for each partition it collects tasks for.
#[derive(Default, Debug)]
#[allow(clippy::module_name_repetitions)]
pub struct FlushQueue {
    items: Vec<Arc<Task>>,
}

impl FlushQueue {
    pub fn iter(&self) -> impl Iterator<Item = &Arc<Task>> + '_ {
        self.items.iter()
    }

    pub fn dequeue(&mut self, cnt: usize) {
        for _ in 0..cnt {
            self.items.remove(0);
        }
    }

    pub fn enqueue(&mut self, item: Arc<Task>) {
        self.items.push(item);
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn size(&self) -> u64 {
        self.items
            .iter()
            .map(|x| x.sealed_memtable.size())
            .map(u64::from)
            .sum::<u64>()
    }
}
