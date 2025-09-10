use crate::Config;
use std::path::Path;

/// Transactional database builder
pub struct Builder(crate::DatabaseBuilder);

impl Builder {
    pub(crate) fn new(path: &Path) -> Self {
        Self(crate::DatabaseBuilder::new(path))
    }

    #[doc(hidden)]
    #[must_use]
    pub fn into_config(self) -> Config {
        self.0.into_config()
    }

    /// Opens the database with transactional semantics, creating it if it does not exist.
    ///
    /// # Errors
    ///
    /// Errors if an I/O error occurred, or if the database can not be opened.
    pub fn open(self) -> crate::Result<crate::TxDatabase> {
        crate::TxDatabase::open(self.0.into_config())
    }

    /// If `false`, write batches or transactions automatically flush data to the operating system.
    ///
    /// Default = false
    ///
    /// Set to `true` to handle persistence manually, e.g. manually using `PersistMode::SyncData` for ACID transactions.
    #[must_use]
    pub fn manual_journal_persist(mut self, flag: bool) -> Self {
        self.0 = self.0.manual_journal_persist(flag);
        self
    }

    /// Sets the amount of flush workers
    ///
    /// Default = # CPU cores
    #[must_use]
    pub fn flush_workers(mut self, n: usize) -> Self {
        self.0 = self.0.flush_workers(n);
        self
    }

    /// Sets the amount of compaction workers
    ///
    /// Default = # CPU cores
    #[must_use]
    pub fn compaction_workers(mut self, n: usize) -> Self {
        self.0 = self.0.compaction_workers(n);
        self
    }

    /// Sets the upper limit for open file descriptors.
    ///
    /// # Panics
    ///
    /// Panics if n < 2.
    #[must_use]
    pub fn max_open_files(mut self, n: usize) -> Self {
        self.0 = self.0.max_open_files(n);
        self
    }

    /// Sets the cache capacity in bytes.
    #[must_use]
    pub fn cache_size(mut self, size_bytes: u64) -> Self {
        self.0 = self.0.cache_size(size_bytes);
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
        self.0 = self.0.max_journaling_size(bytes);
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
        self.0 = self.0.max_write_buffer_size(bytes);
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
        self.0 = self.0.fsync_ms(ms);
        self
    }

    /// Sets the `Database` to clean upon drop.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{PersistMode, Database, KeyspaceCreateOptions};
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
        self.0 = self.0.temporary(flag);
        self
    }
}
