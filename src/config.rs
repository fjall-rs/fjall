// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{journal::error::RecoveryMode, path::absolute_path, Keyspace};
use lsm_tree::{descriptor_table::FileDescriptorTable, Cache};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

/// Global keyspace configuration
#[derive(Clone)]
pub struct Config {
    /// Base path of database
    pub(crate) path: PathBuf,

    /// When true, the path will be deleted upon drop
    pub(crate) clean_path_on_drop: bool,

    pub(crate) cache: Arc<Cache>,

    // TODO: remove in V3
    monkey_patch_cache_size: u64,

    /// Descriptor table that will be shared between partitions
    pub(crate) descriptor_table: Arc<FileDescriptorTable>,

    /// Max size of all journals in bytes
    pub(crate) max_journaling_size_in_bytes: u64, // TODO: should be configurable during runtime: AtomicU64

    /// Max size of all active memtables
    ///
    /// This can be used to cap the memory usage if there are
    /// many (possibly inactive) partitions.
    pub(crate) max_write_buffer_size_in_bytes: u64, // TODO: should be configurable during runtime: AtomicU64

    pub(crate) manual_journal_persist: bool,

    /// Amount of concurrent flush workers
    pub(crate) flush_workers_count: usize,

    /// Amount of compaction workers
    pub(crate) compaction_workers_count: usize,

    /// Fsync every N ms asynchronously
    pub(crate) fsync_ms: Option<u16>,

    pub(crate) journal_recovery_mode: RecoveryMode,
}

const DEFAULT_CPU_CORES: usize = 4;

fn get_open_file_limit() -> usize {
    #[cfg(not(any(target_os = "windows", target_os = "macos")))]
    return 900;

    #[cfg(target_os = "windows")]
    return 400;

    #[cfg(target_os = "macos")]
    return 150;
}

impl Default for Config {
    fn default() -> Self {
        let queried_cores = std::thread::available_parallelism().map(usize::from);

        // Reserve 1 CPU core if possible
        let cpus = (queried_cores.unwrap_or(DEFAULT_CPU_CORES) - 1)
            // Should never be 0
            .max(1);

        Self {
            path: absolute_path(".fjall_data"),
            clean_path_on_drop: false,
            descriptor_table: Arc::new(FileDescriptorTable::new(get_open_file_limit(), 4)),
            max_write_buffer_size_in_bytes: /* 64 MiB */ 64 * 1_024 * 1_024,
            max_journaling_size_in_bytes: /* 512 MiB */ 512 * 1_024 * 1_024,
            fsync_ms: None,
            flush_workers_count: cpus.min(4),
            compaction_workers_count: cpus.min(4),
            journal_recovery_mode: RecoveryMode::default(),
            manual_journal_persist: false,

            cache: Arc::new(Cache::with_capacity_bytes(/* 32 MiB */ 32*1_024*1_024)),
            monkey_patch_cache_size: 0,
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

    /// If `false`, write batches or transactions automatically flush data to the operating system.
    ///
    /// Default = false
    ///
    /// Set to `true` to handle persistence manually, e.g. manually using `PersistMode::SyncData` for ACID transactions.
    #[must_use]
    pub fn manual_journal_persist(mut self, flag: bool) -> Self {
        self.manual_journal_persist = flag;
        self
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
    /// # Panics
    ///
    /// Panics if n < 2.
    #[must_use]
    pub fn max_open_files(mut self, n: usize) -> Self {
        assert!(n >= 2);

        self.descriptor_table = Arc::new(FileDescriptorTable::new(n, 2));
        self
    }

    // TODO: remove in V3
    /// Sets the block cache.
    ///
    /// Defaults to a block cache with 16 MiB of capacity
    /// shared between all partitions inside this keyspace.
    #[must_use]
    #[deprecated = "Use Config::cache_size instead"]
    #[allow(deprecated)]
    pub fn block_cache(mut self, block_cache: Arc<crate::BlockCache>) -> Self {
        self.monkey_patch_cache_size += block_cache.capacity();
        self
    }

    // TODO: remove in V3
    /// Sets the blob cache.
    ///
    /// Defaults to a block cache with 16 MiB of capacity
    /// shared between all partitions inside this keyspace.
    #[must_use]
    #[deprecated = "Use Config::cache_size instead"]
    #[allow(deprecated)]
    pub fn blob_cache(mut self, blob_cache: Arc<crate::BlobCache>) -> Self {
        self.monkey_patch_cache_size += blob_cache.capacity();
        self
    }

    /// Sets the cache capacity in bytes.
    #[must_use]
    pub fn cache_size(mut self, size_bytes: u64) -> Self {
        self.monkey_patch_cache_size = 0;
        self.cache = Arc::new(Cache::with_capacity_bytes(size_bytes));
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
    /// Similar to `db_write_buffer_size` in `RocksDB`, however it is disabled by default in `RocksDB`.
    ///
    /// Set to `u64::MAX` to disable it.
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
    /// persists data to disk (using fsync).
    ///
    /// Default = off
    ///
    /// # Panics
    ///
    /// Panics if ms is 0.
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
    pub fn open(mut self) -> crate::Result<Keyspace> {
        // TODO: remove in V3
        if self.monkey_patch_cache_size > 0 {
            self.cache = Arc::new(Cache::with_capacity_bytes(self.monkey_patch_cache_size));
        }
        Keyspace::open(self)
    }

    /// Opens a transactional keyspace using the config.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    #[cfg(any(feature = "single_writer_tx", feature = "ssi_tx"))]
    pub fn open_transactional(mut self) -> crate::Result<crate::TxKeyspace> {
        // TODO: remove in V3
        if self.monkey_patch_cache_size > 0 {
            self.cache = Arc::new(Cache::with_capacity_bytes(self.monkey_patch_cache_size));
        }
        crate::TxKeyspace::open(self)
    }

    /// Sets the `Keyspace` to clean upon drop.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, PersistMode, Keyspace, PartitionCreateOptions};
    /// # let folder = tempfile::tempdir()?.into_path();
    /// let keyspace = Config::new(&folder).temporary(true).open()?;
    ///
    /// assert!(folder.try_exists()?);
    /// drop(keyspace);
    /// assert!(!folder.try_exists()?);
    /// #
    /// # Ok::<_, fjall::Error>(())
    /// ```
    #[must_use]
    pub fn temporary(mut self, flag: bool) -> Self {
        self.clean_path_on_drop = flag;
        self
    }
}
