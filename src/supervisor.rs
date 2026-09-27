// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    db::Keyspaces,
    flush::manager::FlushManager,
    journal::{manager::JournalManager, Journal},
    locked_file::LockedFileGuard,
    snapshot_tracker::SnapshotTracker,
    write_buffer_manager::WriteBufferManager,
};
use lsm_tree::SequenceNumberCounter;
use std::sync::{Arc, Mutex, RwLock};

pub struct SupervisorInner {
    pub db_config: crate::Config,

    /// Dictionary of all keyspaces
    #[doc(hidden)]
    pub keyspaces: Arc<RwLock<Keyspaces>>,

    pub(crate) write_buffer_size: WriteBufferManager,
    pub(crate) flush_manager: FlushManager,

    pub seqno: SequenceNumberCounter,

    pub snapshot_tracker: SnapshotTracker,

    pub(crate) journal: Arc<Journal>,

    /// Tracks journal size and garbage collects sealed journals when possible
    pub(crate) journal_manager: Arc<RwLock<JournalManager>>,

    pub(crate) backpressure_lock: Mutex<()>,

    #[expect(unused)]
    pub(crate) lock_file: LockedFileGuard,
}

#[derive(Clone)]
pub struct Supervisor(Arc<SupervisorInner>);

impl Drop for SupervisorInner {
    fn drop(&mut self) {
        log::debug!("Dropping supervisor");
    }
}

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
