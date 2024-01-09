use crate::Keyspace;
use lsm_tree::{descriptor_table::FileDescriptorTable, BlockCache};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

/// Global keyspace configuration
#[derive(Clone)]
pub struct Config {
    /// Base path of database
    pub(crate) path: PathBuf,

    /// Block cache that will be shared between partitions
    pub(crate) block_cache: Arc<BlockCache>,

    /// Descriptor table that will be shared between partitions
    pub(crate) descriptor_table: Arc<FileDescriptorTable>,

    /// Max size of all journals in bytes
    pub(crate) max_journaling_size_in_bytes: u64,

    /// Max size of all active memtables
    ///
    /// This can be used to cap the memory usage if there are
    /// many (possibly inactive) partitions.
    pub(crate) max_write_buffer_size_in_bytes: u64,

    /// Amount of compaction workers
    pub(crate) compaction_works_count: usize,

    /// Fsync every N ms asynchronously
    pub(crate) fsync_ms: Option<u16>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            path: ".fjall_data".into(),
            block_cache: Arc::new(BlockCache::with_capacity_bytes(16 * 1_024)),
            descriptor_table: Arc::new(FileDescriptorTable::new(960, 4)),
            max_write_buffer_size_in_bytes: 64 * 1_024 * 1_024,
            max_journaling_size_in_bytes: /* 128 MiB */ 128 * 1_024 * 1_024,
            fsync_ms: Some(1_000),
            compaction_works_count: 4, // TODO: use num_cpu - 1?
        }
    }
}

impl Config {
    /// Creates a new configuration
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        Self {
            path: path.as_ref().into(),
            ..Default::default()
        }
    }

    /// Sets the amount of compaction workers
    ///
    /// Default = 4
    ///
    /// # Panics
    ///
    /// Panics if n is equal to 0.
    #[must_use]
    pub fn compaction_workers(mut self, n: usize) -> Self {
        assert!(n > 0);
        self.compaction_works_count = n;
        self
    }

    /// Sets the upper limit for open file descriptors.
    ///
    /// Default = 960
    ///
    /// # Panics
    ///
    /// Panics if n is equal to 0.
    #[must_use]
    pub fn max_open_files(mut self, n: usize) -> Self {
        self.descriptor_table = Arc::new(FileDescriptorTable::new(n, 4));
        self
    }

    /// Sets the block cache.
    ///
    /// You can create a global [`BlockCache`] and share it between multiple
    /// keyspaces to cap global cache memory usage.
    ///
    /// Defaults to a block cache 16 MiB of capacity shared
    /// between all partitions inside this keyspace.
    #[must_use]
    pub fn block_cache(mut self, block_cache: Arc<BlockCache>) -> Self {
        self.block_cache = block_cache;
        self
    }

    /// Max size of all journals in bytes.
    ///
    /// Default = 128 MiB
    ///
    /// # Panics
    ///
    /// This option should be at least 24 MiB, as one journal takes up at least 16 MiB, so
    /// anything less will immediately stall the system. Otherwise it will panic.
    #[must_use]
    pub fn max_journaling_size(mut self, bytes: u64) -> Self {
        assert!(bytes >= 24 * 1_024 * 1_024);

        self.max_journaling_size_in_bytes = bytes;
        self
    }

    /// Max size of all active memtables in bytes.
    ///
    /// Default = 64 MiB
    #[must_use]
    pub fn max_write_buffer_size(mut self, bytes: u64) -> Self {
        self.max_write_buffer_size_in_bytes = bytes;
        self
    }

    /// If Some, starts an fsync thread that asynchronously
    /// persists data.
    ///
    /// Default = 1 second
    ///
    /// # Panics
    ///
    /// Panics if ms is 0
    #[must_use]
    pub fn fsync_ms(mut self, ms: Option<u16>) -> Self {
        if let Some(ms) = ms {
            assert!(ms > 0);
        }

        self.fsync_ms = ms;
        self
    }

    /// Opens a keyspace using the config.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn open(self) -> crate::Result<Keyspace> {
        Keyspace::open(self)
    }
}
