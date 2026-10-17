// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use lsm_tree::SequenceNumberCounter;

use crate::{
    db::Keyspaces,
    flush::manager::FlushManager,
    journal::{manager::JournalManager, Journal},
    keyspace::InternalKeyspaceId,
    snapshot_tracker::SnapshotTracker,
    worker_pool::WorkerMessage,
    write_buffer_manager::WriteBufferManager,
    Keyspace,
};
use std::{
    collections::{HashSet, VecDeque},
    sync::{Arc, Mutex, RwLock},
};

#[derive(Default)]
struct CompactionState {
    queue: VecDeque<Keyspace>,
    queued: HashSet<InternalKeyspaceId>,
}

/// Keeps one pending compaction per keyspace even when the worker-message
/// channel is full and its wakeup hint cannot be sent.
pub struct CompactionManager(Mutex<CompactionState>);

impl CompactionManager {
    pub(crate) fn new() -> Self {
        Self(Mutex::new(CompactionState::default()))
    }

    fn state(&self) -> std::sync::MutexGuard<'_, CompactionState> {
        #[expect(clippy::expect_used)]
        self.0.lock().expect("compaction queue lock is poisoned")
    }

    pub(crate) fn enqueue(&self, keyspace: Keyspace) -> bool {
        let mut state = self.state();
        if !state.queued.insert(keyspace.id()) {
            return false;
        }
        state.queue.push_back(keyspace);
        true
    }

    pub(crate) fn dequeue(&self) -> Option<Keyspace> {
        let mut state = self.state();
        let keyspace = state.queue.pop_front()?;
        state.queued.remove(&keyspace.id());
        Some(keyspace)
    }

    pub(crate) fn clear(&self) {
        let mut state = self.state();
        state.queue.clear();
        state.queued.clear();
    }
}

pub struct SupervisorInner {
    pub db_config: crate::Config,

    /// Dictionary of all keyspaces
    #[doc(hidden)]
    pub keyspaces: Arc<RwLock<Keyspaces>>,

    pub(crate) write_buffer_size: WriteBufferManager,
    pub(crate) flush_manager: FlushManager,
    pub(crate) compaction_manager: CompactionManager,

    pub seqno: SequenceNumberCounter,

    pub snapshot_tracker: SnapshotTracker,

    pub(crate) journal: Arc<Journal>,

    /// Tracks journal size and garbage collects sealed journals when possible
    pub(crate) journal_manager: Arc<RwLock<JournalManager>>,

    pub(crate) backpressure_lock: Mutex<()>,
}

#[derive(Clone)]
pub struct Supervisor(Arc<SupervisorInner>);

impl std::ops::Deref for Supervisor {
    type Target = SupervisorInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Supervisor {
    pub fn new(inner: SupervisorInner) -> Self {
        Self(Arc::new(inner))
    }

    pub(crate) fn request_compaction(&self, keyspace: &Keyspace) {
        if self.compaction_manager.enqueue(keyspace.clone()) {
            // A full queue only drops the hint; the pending work remains in
            // the manager and is discovered by the worker's fair scheduler.
            keyspace
                .worker_messager
                .try_send(WorkerMessage::Compact(keyspace.clone()))
                .ok();
        }
    }

    pub fn build_seqno_map(
        &self,
        keyspaces: &Keyspaces,
    ) -> Vec<crate::journal::manager::EvictionWatermark> {
        use crate::AbstractTree;

        let mut seqnos = Vec::with_capacity(keyspaces.len());

        for keyspace in keyspaces.values() {
            if let Some(lsn) = keyspace.tree.get_highest_memtable_seqno() {
                seqnos.push(crate::journal::manager::EvictionWatermark {
                    lsn,
                    keyspace: keyspace.clone(),
                });
            }
        }

        seqnos
    }
}
