// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{flush::Task, keyspace::InternalKeyspaceId};
use std::{
    collections::{HashSet, VecDeque},
    sync::{Arc, Mutex, MutexGuard},
};

#[derive(Default)]
struct State {
    queue: VecDeque<Arc<Task>>,
    queued: HashSet<InternalKeyspaceId>,
}

pub struct FlushManager {
    state: Mutex<State>,
}

impl FlushManager {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(State::default()),
        }
    }

    fn state(&self) -> MutexGuard<'_, State> {
        #[expect(clippy::expect_used)]
        self.state.lock().expect("flush queue lock is poisoned")
    }

    pub fn len(&self) -> usize {
        self.state().queue.len()
    }

    pub fn clear(&self) {
        let mut state = self.state();
        state.queue.clear();
        state.queued.clear();
    }

    pub fn wait_for_empty(&self) {
        while self.len() > 0 {
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }

    /// One queued task per keyspace suffices: after a flush, workers requeue
    /// the keyspace if another sealed memtable remains.
    pub fn enqueue(&self, task: Arc<Task>) -> bool {
        let mut state = self.state();
        if !state.queued.insert(task.keyspace.id()) {
            return false;
        }
        state.queue.push_back(task);
        true
    }

    pub fn dequeue(&self) -> Option<Arc<Task>> {
        let mut state = self.state();
        let task = state.queue.pop_front()?;
        state.queued.remove(&task.keyspace.id());
        Some(task)
    }
}
