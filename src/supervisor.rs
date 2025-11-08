use crate::{
    compaction::manager::CompactionManager, flush::new_manager::FlushNewManager,
    journal::manager::JournalManager, snapshot_tracker::SnapshotTracker,
    write_buffer_manager::WriteBufferManager,
};
use std::sync::{Arc, RwLock};

pub struct SupervisorInner {
    pub(crate) write_buffer_size: WriteBufferManager,
    pub(crate) flush_manager: FlushNewManager,
    pub snapshot_tracker: SnapshotTracker,

    /// Tracks journal size and garbage collects sealed journals when possible
    pub(crate) journal_manager: Arc<RwLock<JournalManager>>,

    pub compaction_manager: CompactionManager,
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
