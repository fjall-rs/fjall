// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{read_tx::ReadTransaction, write_tx::WriteTransaction};
use crate::{
    batch::PartitionKey, snapshot_nonce::SnapshotNonce, Config, Keyspace, PartitionOptions,
    PersistMode, TxPartitionHandle,
};
use std::sync::{Arc, Mutex};

/// Transaction keyspace
#[derive(Clone)]
#[allow(clippy::module_name_repetitions)]
pub struct TransactionalKeyspace {
    inner: Keyspace,
    tx_lock: Arc<Mutex<()>>,
}

/// Alias for [`TransactionalKeyspace`]
#[allow(clippy::module_name_repetitions)]
pub type TxKeyspace = TransactionalKeyspace;

impl TxKeyspace {
    /// Starts a new writeable transaction.
    #[must_use]
    pub fn write_tx(&self) -> WriteTransaction {
        let lock = self.tx_lock.lock().expect("lock is poisoned");

        // IMPORTANT: Get the seqno *after* getting the lock
        let instant = self.inner.instant();

        WriteTransaction::new(
            self.inner.clone(),
            lock,
            SnapshotNonce::new(instant, self.inner.snapshot_tracker.clone()),
        )
    }

    /// Starts a new read-only transaction.
    #[must_use]
    pub fn read_tx(&self) -> ReadTransaction {
        let instant = self.inner.instant();

        ReadTransaction::new(SnapshotNonce::new(
            instant,
            self.inner.snapshot_tracker.clone(),
        ))
    }

    /// Flushes the active journal to OS buffers. The durability depends on the [`PersistMode`]
    /// used.
    ///
    /// Persisting only affects durability, NOT consistency! Even without flushing
    /// data is crash-safe.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, PersistMode, Keyspace, PartitionOptions};
    /// # let folder = tempfile::tempdir()?;
    /// let keyspace = Config::new(folder).open_transactional()?;
    /// let items = keyspace.open_partition("my_items", PartitionOptions::default())?;
    ///
    /// items.insert("a", "hello")?;
    ///
    /// keyspace.persist(PersistMode::SyncAll)?;
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

    /// Creates or opens a keyspace partition.
    ///
    /// # Errors
    ///
    /// Returns error, if an IO error occurred.
    ///
    /// # Panics
    ///
    /// Panics if the partition name includes characters other than: a-z A-Z 0-9 _ -
    pub fn open_partition(
        &self,
        name: &str,
        create_options: PartitionOptions,
    ) -> crate::Result<TxPartitionHandle> {
        let partition = self.inner.open_partition(name, create_options)?;

        Ok(TxPartitionHandle {
            inner: partition,
            tx_lock: self.tx_lock.clone(),
        })
    }

    /// Returns the amount of partitions
    #[must_use]
    pub fn partition_count(&self) -> usize {
        self.inner.partition_count()
    }

    /// Gets a list of all partition names in the keyspace
    #[must_use]
    pub fn list_partitions(&self) -> Vec<PartitionKey> {
        self.inner.list_partitions()
    }

    /// Returns `true` if the partition with the given name exists.
    #[must_use]
    pub fn partition_exists(&self, name: &str) -> bool {
        self.inner.partition_exists(name)
    }

    /// Destroys the partition, removing all data associated with it.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn delete_partition(&self, handle: TxPartitionHandle) -> crate::Result<()> {
        self.inner.delete_partition(handle.inner)
    }

    /// Returns the current write buffer size (active + sealed memtables).
    #[must_use]
    pub fn write_buffer_size(&self) -> u64 {
        self.inner.write_buffer_size()
    }

    /// Returns the amount of journals on disk.
    #[must_use]
    pub fn journal_count(&self) -> usize {
        self.inner.journal_count()
    }

    /// Returns the disk space usage of the entire keyspace.
    #[must_use]
    pub fn disk_space(&self) -> u64 {
        self.inner.disk_space()
    }

    /// Opens a keyspace in the given directory.
    ///
    /// # Errors
    ///
    /// Returns error, if an IO error occurred.
    pub fn open(config: Config) -> crate::Result<Self> {
        let inner = Keyspace::create_or_recover(config)?;
        inner.start_background_threads();

        Ok(Self {
            inner,
            tx_lock: Arc::default(),
        })
    }
}
