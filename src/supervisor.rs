// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use lsm_tree::SequenceNumberCounter;

use crate::{
    db::Keyspaces,
    flush::manager::FlushManager,
    journal::{manager::JournalManager, Journal},
    snapshot_tracker::SnapshotTracker,
    write_buffer_manager::WriteBufferManager,
};
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
}
