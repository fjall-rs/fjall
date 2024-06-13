use crate::{journal::shard::RecoveryMode, path::absolute_path, Keyspace};
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
    pub(crate) max_journaling_size_in_bytes: u64, // TODO: should be configurable during runtime: AtomicU64

    /// Max size of all active memtables
    ///
    /// This can be used to cap the memory usage if there are
    /// many (possibly inactive) partitions.
    pub(crate) max_write_buffer_size_in_bytes: u64, // TODO: should be configurable during runtime: AtomicU64

    /// Amount of concurrent flush workers
    pub(crate) flush_workers_count: usize,

    /// Amount of compaction workers
    pub(crate) compaction_workers_count: usize,

    /// Fsync every N ms asynchronously
    pub(crate) fsync_ms: Option<u16>,

    pub(crate) journal_recovery_mode: RecoveryMode,
}

const DEFAULT_CPU_CORES: usize = 4;

impl Default for Config {
    fn default() -> Self {
        let queried_cores = std::thread::available_parallelism().map(usize::from);

        // Reserve 1 CPU core if possible
        let cpus = (queried_cores.unwrap_or(DEFAULT_CPU_CORES) - 1)
            // Should never be 0
            .max(1);

        Self {
            path: absolute_path (".fjall_data"),
            block_cache: Arc::new(BlockCache::with_capacity_bytes(/* 16 MiB */ 16 * 1_024 * 1_024)),
            descriptor_table: Arc::new(FileDescriptorTable::new(960, 2)),
            max_write_buffer_size_in_bytes: 64 * 1_024 * 1_024,
            max_journaling_size_in_bytes: /* 512 MiB */ 512 * 1_024 * 1_024,
            fsync_ms: Some(1_000),
            flush_workers_count: cpus,
            compaction_workers_count: cpus,
            journal_recovery_mode: RecoveryMode::default(),
        }
    }
}

impl Config {
    /// Creates a new configuration
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        Self {
            path: absolute_path(path),
            ..Default::default()
        }
    }

    /// Sets the amount of flush workers
    ///
    /// Default = # CPU cores
    #[must_use]
    pub fn flush_workers(mut self, n: usize) -> Self {
        self.flush_workers_count = n;
        self
    }

    /// Sets the amount of compaction workers
    ///
    /// Default = # CPU cores
    #[must_use]
    pub fn compaction_workers(mut self, n: usize) -> Self {
        self.compaction_workers_count = n;
        self
    }

    /// Sets the upper limit for open file descriptors.
    ///
    /// Default = 960
    ///
    /// # Panics
    ///
    /// Panics if n < 2.
    #[must_use]
    pub fn max_open_files(mut self, n: usize) -> Self {
        assert!(n >= 2);

        self.descriptor_table = Arc::new(FileDescriptorTable::new(n, 2));
        self
    }

    /// Sets the block cache.
    ///
    /// Defaults to a block cache with 16 MiB of capacity
    /// shared between all partitions inside this keyspace.
    #[must_use]
    pub fn block_cache(mut self, block_cache: Arc<BlockCache>) -> Self {
        self.block_cache = block_cache;
        self
    }

    /// Max size of all journals in bytes.
    ///
    /// Default = 512 MiB
    ///
    /// # Panics
    ///
    /// Panics if bytes < 24 MiB.
    ///
    /// This option should be at least 24 MiB, as one journal takes up at least 16 MiB, so
    /// anything less will immediately stall the system.
    ///
    /// Same as `max_total_wal_size` in `RocksDB`.
    #[must_use]
    pub fn max_journaling_size(mut self, bytes: u64) -> Self {
        assert!(bytes >= 24 * 1_024 * 1_024);

        self.max_journaling_size_in_bytes = bytes;
        self
    }

    /// Max size of all memtables in bytes.
    ///
    /// Similar to `db_write_buffer_size` in `RocksDB`.
    ///
    /// Default = 64 MiB
    ///
    /// # Panics
    ///
    /// Panics if bytes < 1 MiB.
    #[must_use]
    pub fn max_write_buffer_size(mut self, bytes: u64) -> Self {
        assert!(bytes >= 1_024 * 1_024);

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

    /// Opens a transactional keyspace using the config.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    #[cfg(feature = "single_writer_tx")]
    pub fn open_transactional(self) -> crate::Result<crate::TxKeyspace> {
        crate::TxKeyspace::open(self)
    }
}
