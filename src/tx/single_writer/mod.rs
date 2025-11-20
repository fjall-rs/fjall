// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

mod keyspace;
mod write_tx;

use crate::{
    keyspace::KeyspaceKey, Config, Database, KeyspaceCreateOptions, PersistMode, Snapshot,
};
use std::{
    path::Path,
    sync::{Arc, Mutex},
};

pub use keyspace::SingleWriterTxKeyspace;
pub use write_tx::WriteTransaction;

pub trait Openable {
    fn open(config: Config) -> crate::Result<Self>
    where
        Self: Sized;
}

/// Single-writer transactional database
#[derive(Clone)]
#[allow(clippy::module_name_repetitions)]
pub struct TxDatabase {
    pub(crate) inner: Database,
    single_writer_lock: Arc<Mutex<()>>,
}

impl Openable for TxDatabase {
    fn open(config: Config) -> crate::Result<Self>
    where
        Self: Sized,
    {
        Ok(Self {
            inner: crate::Database::open(config)?,
            single_writer_lock: Arc::default(),
        })
    }
}

impl TxDatabase {
    /// Creates a new database builder to create or open a database at `path`.
    pub fn builder(path: impl AsRef<Path>) -> crate::DatabaseBuilder<Self> {
        crate::DatabaseBuilder::new(path.as_ref())
    }

    #[doc(hidden)]
    #[must_use]
    pub fn inner(&self) -> &Database {
        &self.inner
    }

    /// Starts a new writeable transaction.
    #[must_use]
    #[allow(clippy::missing_panics_doc)]
    pub fn write_tx(&self) -> WriteTransaction<'_> {
        #[allow(clippy::expect_used)]
        let guard = self.single_writer_lock.lock().expect("poisoned tx lock");

        let mut write_tx = WriteTransaction::new(
            self.clone(),
            self.inner.supervisor.snapshot_tracker.open(),
            guard,
        );

        if !self.inner.config.manual_journal_persist {
            write_tx = write_tx.durability(Some(PersistMode::Buffer));
        }

        write_tx
    }

    /// Starts a new read-only transaction (a.k.a. [`Snapshot`]).
    #[must_use]
    pub fn read_tx(&self) -> Snapshot {
        self.inner.snapshot()
    }

    /// Flushes the active journal. The durability depends on the [`PersistMode`]
    /// used.
    ///
    /// Persisting only affects durability, NOT consistency! Even without flushing
    /// data is crash-safe.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{PersistMode, SingleWriterTxDatabase, KeyspaceCreateOptions};
    /// # let folder = tempfile::tempdir()?;
    /// let db = SingleWriterTxDatabase::builder(folder).open()?;
    /// let items = db.keyspace("my_items", KeyspaceCreateOptions::default)?;
    ///
    /// items.insert("a", "hello")?;
    ///
    /// db.persist(PersistMode::SyncAll)?;
    /// #
    /// # Ok::<_, fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Returns error, if an IO error occurred.
    pub fn persist(&self, mode: PersistMode) -> crate::Result<()> {
        self.inner.persist(mode)
    }

    /// Creates or opens a keyspace.
    ///
    /// If the keyspace does not yet exist, it will be created configured with `create_options`.
    /// Otherwise simply a handle to the existing keyspace will be returned.
    ///
    /// Keyspace names can be up to 255 characters long and can not be empty.
    ///
    /// # Errors
    ///
    /// Returns error, if an IO error occurred.
    ///
    /// # Panics
    ///
    /// Panics if the keyspace name is invalid.
    pub fn keyspace(
        &self,
        name: &str,
        create_options: impl FnOnce() -> KeyspaceCreateOptions,
    ) -> crate::Result<SingleWriterTxKeyspace> {
        let keyspace = self.inner.keyspace(name, create_options)?;

        Ok(SingleWriterTxKeyspace {
            inner: keyspace,
            db: self.clone(),
        })
    }

    /// Returns the number of keyspaces.
    #[must_use]
    pub fn keyspace_count(&self) -> usize {
        self.inner.keyspace_count()
    }

    /// Gets a list of all keyspace names in the database.
    #[must_use]
    pub fn list_keyspaces(&self) -> Vec<KeyspaceKey> {
        self.inner.list_keyspaces()
    }

    /// Returns `true` if the keyspace with the given name exists.
    #[must_use]
    pub fn keyspace_exists(&self, name: &str) -> bool {
        self.inner.keyspace_exists(name)
    }

    /// Returns the current write buffer size (active + sealed memtables).
    #[must_use]
    pub fn write_buffer_size(&self) -> u64 {
        self.inner.write_buffer_size()
    }

    /// Returns the number of journal fragments on disk.
    #[must_use]
    pub fn journal_count(&self) -> usize {
        self.inner.journal_count()
    }

    /// Returns the disk space usage of the entire database.
    ///
    /// # Errors
    ///
    /// Returns error, if an IO error occurred.
    pub fn disk_space(&self) -> crate::Result<u64> {
        self.inner.disk_space()
    }

    /// Opens a database in the given directory.
    ///
    /// # Errors
    ///
    /// Returns error, if an IO error occurred.
    pub fn open(config: Config) -> crate::Result<Self> {
        let inner = Database::create_or_recover(config)?;

        Ok(Self {
            inner,
            single_writer_lock: Arc::default(),
        })
    }
}
