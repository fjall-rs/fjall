use crate::Keyspace;
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
        write!(f, "FlushTask({}:{})", self.keyspace.name, self.id)
    }
}
