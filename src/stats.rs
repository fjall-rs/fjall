// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{Database, Keyspace};
use std::{
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering::Relaxed},
        OnceLock,
    },
    time::Duration,
};

/// Ephemeral, runtime stats
#[derive(Default)]
pub struct Stats {
    /// Active compaction conter
    pub(crate) active_compaction_count: AtomicUsize,

    /// Time spent in compactions (in µs)
    pub(crate) time_compacting: AtomicU64,

    /// Number of completed compactions
    pub(crate) compactions_completed: AtomicUsize,

    /// Set once the WAL has been recovered when opening the database
    pub(crate) wal_recovery: OnceLock<WalRecoveryStats>,
}

/// Statistics of the write-ahead log recovery when the database was opened
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WalRecoveryStats {
    /// Time it took to recover the write-ahead log
    pub duration: Duration,

    /// Number of items replayed into memtables
    pub replayed_count: usize,
}

/// Runtime statistics and disk usage of a database, see [`Database::stats`]
#[derive(Clone, Copy)]
pub struct DatabaseStats<'a> {
    pub(crate) db: &'a Database,

    /// Statistics of the write-ahead log recovery when the database was opened.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Database, DurabilityLevel, KeyspaceCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// {
    ///     let db = Database::builder(&folder).open()?;
    ///     let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
    ///     tree.insert("a", "abc", DurabilityLevel::Buffer)?;
    ///     tree.insert("b", "abc", DurabilityLevel::Buffer)?;
    /// }
    ///
    /// let db = Database::builder(&folder).open()?;
    /// assert_eq!(2, db.stats().wal_recovery.replayed_count);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    pub wal_recovery: WalRecoveryStats,
}

impl DatabaseStats<'_> {
    /// Returns the current write buffer size (active + sealed memtables).
    #[must_use]
    pub fn write_buffer_size(&self) -> u64 {
        self.db.supervisor.write_buffer_size.get()
    }

    /// Returns the time all compactions took until now.
    ///
    /// # Experimental
    ///
    /// This is a non-stable API currently.
    #[doc(hidden)]
    #[must_use]
    pub fn time_compacting(&self) -> Duration {
        Duration::from_micros(self.db.stats.time_compacting.load(Relaxed))
    }

    /// Returns the number of active compactions currently running.
    ///
    /// # Experimental
    ///
    /// This is a non-stable API currently.
    #[doc(hidden)]
    #[must_use]
    pub fn active_compactions(&self) -> usize {
        self.db.stats.active_compaction_count.load(Relaxed)
    }

    /// Returns the number of completed compactions.
    ///
    /// # Experimental
    ///
    /// This is a non-stable API currently.
    #[doc(hidden)]
    #[must_use]
    pub fn compactions_completed(&self) -> usize {
        self.db.stats.compactions_completed.load(Relaxed)
    }

    /// Returns the number of write-ahead log fragments on disk.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::Database;
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = Database::builder(folder).open()?;
    /// assert_eq!(1, db.stats().wal_count());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    #[must_use]
    #[expect(clippy::expect_used, clippy::missing_panics_doc)]
    pub fn wal_count(&self) -> usize {
        // 1 = active WAL fragment
        1 + self
            .db
            .supervisor
            .ledger()
            .expect("should not be poisoned")
            .wal
            .sealed_fragment_count()
    }

    /// Returns the disk space usage of the write-ahead log.
    ///
    /// # Errors
    ///
    /// Returns error, if an IO error occurred.
    #[doc(hidden)]
    pub fn wal_disk_space(&self) -> crate::Result<u64> {
        self.db.supervisor.ledger()?.wal.disk_usage()
    }

    /// Returns the disk space usage of the database manifest.
    ///
    /// # Errors
    ///
    /// Returns error, if an IO error occurred.
    #[doc(hidden)]
    pub fn manifest_disk_space(&self) -> crate::Result<u64> {
        self.db.supervisor.ledger()?.manifest.disk_usage()
    }

    /// Returns the disk space usage of the entire database.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Database, DurabilityLevel, KeyspaceCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = Database::builder(folder).open()?;
    /// let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
    /// assert_eq!(0, db.stats().disk_space()?);
    ///
    /// tree.insert("a", "abc", DurabilityLevel::Buffer)?;
    /// assert!(db.stats().disk_space()? > 0);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Returns error, if an IO error occurred.
    #[expect(clippy::expect_used, clippy::missing_panics_doc)]
    pub fn disk_space(&self) -> crate::Result<u64> {
        let wal_size = self.wal_disk_space()?;

        let keyspaces_size = self
            .db
            .supervisor
            .keyspaces
            .read()
            .expect("lock is poisoned")
            .values()
            .map(Keyspace::disk_space)
            .sum::<u64>();

        Ok(wal_size + keyspaces_size)
    }

    /// Returns the capacity of the block cache in bytes.
    #[doc(hidden)]
    #[must_use]
    pub fn cache_capacity(&self) -> u64 {
        self.db.config.cache.capacity()
    }

    /// Returns the current size of the block cache in bytes.
    #[doc(hidden)]
    #[must_use]
    pub fn cache_size(&self) -> u64 {
        self.db.config.cache.size()
    }
}
