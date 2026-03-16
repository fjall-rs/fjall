// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::flush::Task;
use std::sync::Arc;

pub struct FlushManager {
    sender: flume::Sender<Arc<Task>>,
    receiver: flume::Receiver<Arc<Task>>,
}

impl FlushManager {
    pub fn new() -> Self {
        // Unbounded: flush tasks are tiny (Arc<Task>) and bounded by memtable
        // rotation rate. A bounded channel here participated in the deadlock
        // chain (#260) — workers blocked on flush enqueue while holding the
        // worker channel slot needed for Close delivery.
        let (tx, rx) = flume::unbounded();

        Self {
            sender: tx,
            receiver: rx,
        }
    }

    pub fn len(&self) -> usize {
        self.receiver.len()
    }

    pub fn clear(&self) {
        let _ = self.receiver.drain().count();
    }

    pub fn wait_for_empty(&self) {
        while !self.receiver.is_empty() {
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }

    pub fn enqueue(&self, task: Arc<Task>) {
        self.sender.send(task).ok();
    }

    pub fn dequeue(&self) -> Option<Arc<Task>> {
        self.receiver.try_recv().ok()
    }
}
