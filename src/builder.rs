// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{db_config::CompactionFilterAssigner, Config};
use lsm_tree::{Cache, CompressionType, DescriptorTable};
use std::{marker::PhantomData, path::Path, sync::Arc};

pub trait Openable {
    fn open(config: Config) -> crate::Result<Self>
    where
        Self: Sized;
}

/// Database builder
pub struct Builder<O: Openable> {
    inner: Config,
    _phantom: PhantomData<O>,
}

impl<O: Openable> Builder<O> {
    pub(crate) fn new(path: &Path) -> Self {
        Self {
            inner: Config::new(path),
            _phantom: PhantomData,
        }
    }

    #[doc(hidden)]
    #[must_use]
    pub fn into_config(self) -> Config {
        self.inner
    }

    /// Opens the database, creating it if it does not exist.
    ///
    /// # Errors
    ///
    /// Errors if an I/O error occurred, or if the database can not be opened.
    pub fn open(self) -> crate::Result<O> {
        O::open(self.inner)
    }

    /// Sets the cache capacity in bytes.
    ///
    /// It is recommended to configure the block cache capacity to be ~20-25% of the available memory - or more **if** the data set _fully_ fits into memory.
    #[must_use]
    pub fn cache_size(mut self, size_bytes: u64) -> Self {
        self.inner.cache = Arc::new(Cache::with_capacity_bytes(size_bytes));
        self
    }

    /// Sets the compression type to use for large values that are written into the write-ahead log.
    #[must_use]
    pub fn wal_compression(mut self, comp: CompressionType) -> Self {
        self.inner.wal_compression_type = comp;
        self
    }

    /// Sets the maximum number of journal bytes processed by one write-pipeline leader term.
    ///
    /// Default = 1 MB
    ///
    /// # Caution
    ///
    /// This feature is experimental.
    #[doc(hidden)]
    #[must_use]
    pub fn write_pipeline_leader_capacity(mut self, bytes: usize) -> Self {
        self.inner.write_pipeline_leader_capacity = bytes;
        self
    }

    /// Sets the number of worker threads.
    ///
    /// Default = min(# CPU cores, 4)
    ///
    /// # Panics
    ///
    /// Panics, if below 1.
    #[must_use]
    pub fn worker_threads(self, n: usize) -> Self {
        #[cfg(not(test))]
        assert!(n > 0, "worker count must be at least 1");

        self.worker_threads_unchecked(n)
    }

    #[doc(hidden)]
    #[must_use]
    pub fn worker_threads_unchecked(mut self, n: usize) -> Self {
        self.inner.worker_threads = n;
        self
    }

    /// Disables write stalls and write halts.
    ///
    /// Only meant for deterministic testing.
    #[doc(hidden)]
    #[must_use]
    pub fn disable_write_stalls(mut self, flag: bool) -> Self {
        self.inner.disable_write_stalls = flag;
        self
    }

    /// Sets the size (in bytes) above which the DB manifest is rotated.
    ///
    /// Only meant for testing.
    #[doc(hidden)]
    #[must_use]
    pub fn manifest_rotation_threshold(mut self, bytes: u64) -> Self {
        self.inner.manifest_rotation_threshold = bytes;
        self
    }

    /// Sets the upper limit for cached file descriptors.
    ///
    /// All user data stored in multiple, immutable files.
    /// Large database can approach hundreds or thousands of files; for IO-bound workloads
    /// this may require many `fopen()` (or equivalent) syscalls.
    ///
    /// Caching files descriptors reduces the impact of such syscalls, at the cost of more open file descriptors.
    ///
    /// # Note
    ///
    /// Setting to None is currently not supported.
    ///
    /// # Panics
    ///
    /// Panics if n < 10 or `None`.
    #[must_use]
    pub fn max_cached_files(mut self, n: Option<usize>) -> Self {
        self.inner.descriptor_table = n.map(|n| Arc::new(DescriptorTable::new(n)));
        self
    }

    /// Maximum size of all journals in bytes.
    ///
    /// Default = 512 MiB
    ///
    /// # Panics
    ///
    /// Panics if < 64 MiB.
    ///
    /// Same as `max_total_wal_size` in `RocksDB`.
    #[must_use]
    pub fn max_journaling_size(mut self, bytes: u64) -> Self {
        assert!(bytes >= 64 * 1_024 * 1_024);

        self.inner.max_journaling_size_in_bytes = bytes;
        self
    }

    /// Maximum size of all memtables in bytes.
    ///
    /// Similar to `db_write_buffer_size` in `RocksDB`, however it is disabled by default in `RocksDB`.
    ///
    /// Set to `u64::MAX` or `0` to disable it.
    ///
    /// Default = off
    ///
    /// # Panics
    ///
    /// Panics if bytes < 1 MiB.
    #[doc(hidden)]
    #[must_use]
    #[deprecated = "todo"]
    pub fn max_write_buffer_size(mut self, bytes: Option<u64>) -> Self {
        if let Some(bytes) = bytes {
            assert!(bytes >= 1_024 * 1_024);
        }

        self.inner.max_write_buffer_size_in_bytes = bytes;
        self
    }

    /// Sets the `Database` to clean upon drop.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{DurabilityLevel, Database, KeyspaceCreateOptions};
    /// # let folder = tempfile::tempdir()?.into_path();
    /// let db = Database::builder(&folder).temporary(true).open()?;
    ///
    /// assert!(folder.try_exists()?);
    /// drop(db);
    /// assert!(!folder.try_exists()?);
    /// #
    /// # Ok::<_, fjall::Error>(())
    /// ```
    #[must_use]
    pub fn temporary(mut self, flag: bool) -> Self {
        self.inner.clean_path_on_drop = flag;
        self
    }

    /// Installs a factory that assigns compaction filters to new or recovered keyspaces.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{DurabilityLevel, Database, KeyspaceCreateOptions};
    /// # use std::sync::Arc;
    /// # let folder = tempfile::tempdir()?.keep();
    /// use lsm_tree::compaction::filter::Factory;
    ///
    /// let db = Database::builder(&folder)
    ///     .temporary(true)
    ///     .with_compaction_filter_factories(
    ///         Arc::new(|keyspace| {
    ///             // Match on the keyspace name to assign specific compaction filters
    ///             todo!()
    ///         })
    ///     )
    ///     .open()?;
    ///
    /// #
    /// # Ok::<_, fjall::Error>(())
    /// ```
    #[must_use]
    pub fn with_compaction_filter_factories(mut self, f: CompactionFilterAssigner) -> Self {
        self.inner.compaction_filter_factory_assigner = Some(f);
        self
    }
}
