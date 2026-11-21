// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::Config;
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
    #[doc(hidden)]
    #[must_use]
    pub fn disable_write_stalls(mut self, flag: bool) -> Self {
        self.inner.disable_write_stalls = flag;
        self
    }

    /// Sets the size (in bytes) above which the DB manifest is rotated.
    #[doc(hidden)]
    #[must_use]
    pub fn manifest_rotation_threshold(mut self, bytes: u64) -> Self {
        self.inner.manifest_rotation_threshold = bytes;
        self
    }

    /// Sets the size (in bytes) above which the active WAL fragment is rotated.
    #[doc(hidden)]
    #[must_use]
    pub fn wal_rotation_threshold(mut self, bytes: u64) -> Self {
        self.inner.wal_rotation_threshold = bytes;
        self
    }

    /// Sets the value size (in bytes) from which values are compressed in the write-ahead log,
    /// see [`Builder::wal_compression`].
    #[doc(hidden)]
    #[must_use]
    pub fn wal_compression_threshold(mut self, bytes: u32) -> Self {
        self.inner.wal_compression_threshold = bytes;
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

    /// Sets the maximum size of all write-ahead log fragments in bytes.
    ///
    /// Default = 512 MiB
    ///
    /// Same as `max_total_wal_size` in `RocksDB`.
    ///
    /// # Panics
    ///
    /// Panics if < 64 MiB.
    #[must_use]
    pub fn max_wal_size(self, bytes: u64) -> Self {
        assert!(bytes >= 64 * 1_024 * 1_024);

        self.max_wal_size_unchecked(bytes)
    }

    #[doc(hidden)]
    #[must_use]
    pub fn max_wal_size_unchecked(mut self, bytes: u64) -> Self {
        self.inner.max_journaling_size_in_bytes = bytes;
        self
    }

    /// Sets the maximum size of all memtables in bytes.
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

    /// Adds a function that assigns compaction filter factories to new or recovered
    /// keyspaces, by keyspace name.
    ///
    /// Can be called multiple times, e.g. once for the application's own keyspaces and once
    /// for a library that shares the database.
    /// The functions are asked in the order they were added, and the first one that
    /// returns a factory wins.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::Database;
    /// use lsm_tree::compaction::filter::Factory;
    /// use std::sync::Arc;
    /// // use third_party_library::some_other_filters;
    /// #
    /// # // e.g. exported by a library that shares the database
    /// # fn some_other_filters(keyspace: &str) -> Option<Arc<dyn Factory>> {
    /// #   match keyspace {
    /// #        "library_items" => todo!(),
    /// #        _ => None,
    /// #    }
    /// # }
    ///
    /// # let folder = tempfile::tempdir()?;
    /// let db = Database::builder(&folder)
    ///     .with_compaction_filters(|keyspace| match keyspace {
    ///         "my_items" => todo!(),
    ///         _ => None,
    ///     })
    ///     .with_compaction_filters(some_other_filters)
    ///     .open()?;
    /// #
    /// # Ok::<_, fjall::Error>(())
    /// ```
    #[must_use]
    pub fn with_compaction_filters(
        mut self,
        f: impl Fn(&str) -> Option<Arc<dyn lsm_tree::compaction::filter::Factory>>
            + Send
            + Sync
            + 'static,
    ) -> Self {
        self.inner.compaction_filter_assigners.push(Arc::new(f));
        self
    }
}
