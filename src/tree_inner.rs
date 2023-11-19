use crate::{block_cache::BlockCache, commit_log::CommitLog, memtable::MemTable, Config};
use std::sync::{atomic::AtomicU64, Arc};

pub struct TreeInner {
    pub(crate) config: Config,
    pub(crate) active_memtable: Arc<MemTable>,
    pub(crate) commit_log: Arc<CommitLog>,
    pub(crate) block_cache: Arc<BlockCache>,
    pub(crate) lsn: AtomicU64,
}
