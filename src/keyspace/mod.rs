// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

mod config;
pub mod name;
pub mod options;
mod write_delay;

#[cfg(test)]
mod test;

use crate::manifest::ManifestPersister;
use crate::write_pipeline::{WriteRecord, WriteRecordPayload};
use crate::DurabilityLevel;
use crate::{
    ingestion::Ingestion, locked_file::LockedFileGuard, poison::PoisonSignal, stats::Stats,
    supervisor::Supervisor, worker_pool::WorkerMessage, Database, Guard, Iter,
};
use lsm_tree::{AbstractTree, AnyTree, SeqNo, UserKey, UserValue};
use options::CreateOptions;
use std::{
    ops::RangeBounds,
    path::Path,
    sync::{atomic::AtomicBool, Arc, RwLock},
    time::Duration,
};
use write_delay::perform_write_stall;

/// Keyspace key (a.k.a. column family, locality group)
pub type KeyspaceKey = byteview::StrView;

pub type InternalKeyspaceId = u64;

pub fn apply_to_base_config(
    config: lsm_tree::Config,
    our_config: &CreateOptions,
) -> lsm_tree::Config {
    config
        // .level_count(our_config.level_count)
        .data_block_size_policy(our_config.data_block_size_policy.clone())
        .data_block_compression_policy(our_config.data_block_compression_policy.clone())
        .index_block_compression_policy(our_config.index_block_compression_policy.clone())
        .data_block_restart_interval_policy(our_config.data_block_restart_interval_policy.clone())
        // .index_block_restart_interval_policy(our_config.index_block_restart_interval_policy.clone())
        .filter_block_pinning_policy(our_config.filter_block_pinning_policy.clone())
        .index_block_pinning_policy(our_config.index_block_pinning_policy.clone())
        .data_block_hash_ratio_policy(our_config.data_block_hash_ratio_policy.clone())
        .expect_point_read_hits(our_config.expect_point_read_hits)
        .with_kv_separation(our_config.kv_separation_opts.clone())
        .index_block_partitioning_policy(our_config.index_block_partitioning_policy.clone())
        .filter_block_partitioning_policy(our_config.filter_block_partitioning_policy.clone())
        .filter_policy(our_config.filter_policy.clone())
        .with_compaction_filter_factory(our_config.compaction_filter_factory.clone())
}

pub struct KeyspaceInner {
    /// Internal ID
    pub(crate) id: InternalKeyspaceId,

    // Internal
    //
    /// Keyspace name
    pub(crate) name: KeyspaceKey,

    // Keyspace configuration
    #[doc(hidden)]
    pub config: CreateOptions,

    /// If `true`, the keyspace is marked as deleted
    pub(crate) is_deleted: AtomicBool,

    /// If `true`, fsync failed during persisting, see `Error::Poisoned`
    pub(crate) is_poisoned: PoisonSignal,

    /// The actual LSM-tree
    #[doc(hidden)]
    pub tree: AnyTree,

    pub(crate) version_set_provider: Arc<lsm_tree::IoSuperVersionSetProvider>,

    pub(crate) compaction_lock: RwLock<()>,

    pub(crate) supervisor: Supervisor,

    /// Database-level stats
    pub(crate) stats: Arc<Stats>,

    pub(crate) worker_messager: flume::Sender<WorkerMessage>,

    #[expect(unused)]
    lock_file: LockedFileGuard,
}

impl Drop for KeyspaceInner {
    fn drop(&mut self) {
        log::trace!("Dropping KeyspaceInner: {:?}", self.name);

        // NOTE: A deleted keyspace's files are already marked as deleted (see `Database::delete_keyspace`),
        // they are removed once the last version referencing them is dropped

        #[cfg(feature = "__internal_whitebox")]
        crate::drop::decrement_drop_counter();
    }
}

/// Handle to a keyspace
///
/// Each keyspace is backed by an LSM-tree to provide a
/// disk-backed search tree, and can be configured individually.
///
/// A keyspace generally only takes a little bit of memory and disk space,
/// but does not spawn its own background threads.
///
/// As long as a handle to a keyspace is held, its folder is not cleaned up from disk
/// in case it is deleted from another thread.
#[derive(Clone)]
#[doc(alias = "column family")]
#[doc(alias = "locality group")]
#[doc(alias = "table")]
pub struct Keyspace(pub(crate) Arc<KeyspaceInner>);

impl AsRef<Keyspace> for &Keyspace {
    fn as_ref(&self) -> &Keyspace {
        self
    }
}

impl std::ops::Deref for Keyspace {
    type Target = KeyspaceInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl PartialEq for Keyspace {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for Keyspace {}

impl std::hash::Hash for Keyspace {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        state.write(self.name.as_bytes());
    }
}

impl Keyspace {
    #[doc(hidden)]
    #[must_use]
    pub fn id(&self) -> InternalKeyspaceId {
        self.id
    }

    /// Returns the keyspace's name.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Database, KeyspaceCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = Database::builder(folder).open()?;
    /// let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
    /// assert_eq!("default", &**tree.name());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    #[must_use]
    pub fn name(&self) -> &KeyspaceKey {
        &self.name
    }

    /// Clears the entire keyspace in O(1) time.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Database, KeyspaceCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = Database::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
    /// tree.insert("a", "a")?;
    /// assert!(tree.contains_key("a")?);
    ///
    /// tree.clear()?;
    /// assert!(!tree.contains_key("a")?);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Note
    ///
    /// Memtable contents are deleted as well.
    /// This operation is thus not equal to [`Keyspace::drop_range(..)`](Keyspace::drop_range).
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn clear(&self) -> crate::Result<()> {
        // IMPORTANT: Exclusive, otherwise a running compaction installs its result
        // (made from the cleared tables) after the clear
        let _compaction_lock = self
            .compaction_lock
            .write()
            .map_err(|_| crate::Error::Poisoned)?;

        if self.is_deleted.load(std::sync::atomic::Ordering::Acquire) {
            return Err(crate::Error::KeyspaceDeleted);
        }

        // IMPORTANT: Writes are applied to memtables while holding the ledger lock,
        // which `install_version` holds, so the clear is serialized with all writes
        //
        // Appends the version edit (drop all tables & blob files) to the manifest,
        // then installs a new, empty version
        let freed_bytes = self.install_version(|persister, seqno| {
            self.version_set_provider
                .clear(persister, &self.tree, seqno)
        })?;

        self.supervisor.write_buffer_size.free(freed_bytes);

        // NOTE: The clear raised the flush watermark, so WAL fragments may not be needed anymore
        self.supervisor.prune_wal(&mut self.supervisor.ledger()?)?;

        Ok(())
    }

    /// Removes old versions (and with them, old memtables and deleted files)
    /// that no snapshot can read anymore.
    ///
    /// Called after installing a new version.
    pub(crate) fn prune_versions(&self) {
        let gc_watermark = self.supervisor.snapshot_tracker.get_seqno_safe_to_gc();
        self.version_set_provider.prune(gc_watermark);
    }

    /// Installs a new version of the keyspace's tree, with a new version seqno.
    ///
    /// `install` is called with the ledger lock held, and returns what `install_version` returns.
    pub(crate) fn install_version<T>(
        &self,
        install: impl FnOnce(&mut ManifestPersister<'_>, SeqNo) -> crate::Result<T>,
    ) -> crate::Result<T> {
        let mut ledger = self.supervisor.ledger()?;

        // IMPORTANT: Check the poisoned flag after getting the ledger lock, otherwise TOCTOU
        if self.is_poisoned.is_poisoned() {
            return Err(crate::Error::Poisoned);
        }

        let seqno = self.supervisor.seqno.next();

        let result = install(&mut ledger.persister(self.id), seqno).inspect_err(|e| {
            log::error!("Installing a new version failed, which is a FATAL failure: {e:?}");
            self.is_poisoned.poison();
        })?;

        self.supervisor.snapshot_tracker.publish(seqno);

        self.prune_versions();

        self.supervisor
            .maybe_rotate_manifest(&mut ledger, &self.is_poisoned)?;

        Ok(result)
    }

    /// Drops all tables that are fully contained in the given key range.
    ///
    /// Tables that only partially overlap the range are kept, so keys in the range
    /// may still be readable afterwards.
    ///
    /// # Note
    ///
    /// Memtable contents overlapping the range are not deleted.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    #[doc(hidden)]
    pub fn drop_range<K: AsRef<[u8]>, R: RangeBounds<K>>(&self, range: R) -> crate::Result<()> {
        // NOTE: Exclusive, like a major compaction
        let _compaction_lock = self
            .compaction_lock
            .write()
            .map_err(|_| crate::Error::Poisoned)?;

        if self.is_deleted.load(std::sync::atomic::Ordering::Acquire) {
            return Err(crate::Error::KeyspaceDeleted);
        }

        let gc_watermark = self.supervisor.snapshot_tracker.get_seqno_safe_to_gc();

        // NOTE: Dropping tables doesn't rewrite any data, so it's fine to hold the locks while doing it
        self.install_version(|persister, seqno| {
            self.version_set_provider
                .drop_range(persister, &self.tree, range, seqno, gc_watermark)
        })?;

        Ok(())
    }

    /// Returns the number of blob bytes on disk that are not referenced.
    ///
    /// These will be reclaimed over time by blob garbage collection automatically.
    #[must_use]
    pub fn fragmented_blob_bytes(&self) -> u64 {
        self.tree.stale_blob_bytes()
    }

    #[doc(hidden)]
    #[must_use]
    pub fn sealed_memtable_count(&self) -> usize {
        self.tree.sealed_memtable_count()
    }

    /// Prepare ingestiom of a pre-sorted stream of key-value pairs into the keyspace.
    ///
    /// Prefer this method over singular inserts or write batches/transactions
    /// for maximum bulk load speed.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    ///
    /// # Panics
    ///
    /// Panics if the input iterator is not sorted in ascending order.
    pub fn start_ingestion(&self) -> crate::Result<Ingestion<'_>> {
        Ingestion::new(self)
    }

    pub(crate) fn from_database(
        keyspace_id: InternalKeyspaceId,
        db: &Database,
        tree: AnyTree,
        name: KeyspaceKey,
        config: CreateOptions,
        version_set_provider: Arc<lsm_tree::IoSuperVersionSetProvider>,
    ) -> Self {
        Self(Arc::new(KeyspaceInner {
            id: keyspace_id,
            name,
            tree,
            config,
            supervisor: db.supervisor.clone(),
            worker_messager: db.worker_pool.sender.clone(),
            is_deleted: AtomicBool::default(),
            is_poisoned: db.is_poisoned.clone(),
            lock_file: db.lock_file.clone(),
            stats: db.stats.clone(),
            version_set_provider,
            compaction_lock: RwLock::default(),
        }))
    }

    /// Creates a new keyspace.
    pub(crate) fn create_new(
        keyspace_id: InternalKeyspaceId,
        db: &Database,
        name: KeyspaceKey,
        config: CreateOptions,
    ) -> crate::Result<Self> {
        log::debug!("Creating keyspace {keyspace_id}={name:?}");

        let version_set_provider = Arc::new(lsm_tree::IoSuperVersionSetProvider::new());

        let base_config = lsm_tree::Config::new(db.path(), version_set_provider.clone())
            .use_descriptor_table(db.config.descriptor_table.clone())
            .use_cache(db.config.cache.clone())
            .with_table_id_counter(db.table_id_counter.clone())
            .with_blob_file_id_counter(db.blob_file_id_counter.clone());

        let base_config = apply_to_base_config(base_config, &config);
        let tree = base_config.open()?;

        Ok(Self(Arc::new(KeyspaceInner {
            supervisor: db.supervisor.clone(),
            worker_messager: db.worker_pool.sender.clone(),
            id: keyspace_id,
            name,
            config,
            tree,
            is_deleted: AtomicBool::default(),
            is_poisoned: db.is_poisoned.clone(),
            stats: db.stats.clone(),
            lock_file: db.lock_file.clone(),
            version_set_provider,
            compaction_lock: RwLock::default(),
        })))
    }

    /// Returns the metrics struct of the underlying LSM-tree.
    ///
    /// # Note
    ///
    /// This function is experimental and metric names may change in future releases.
    #[cfg(feature = "metrics")]
    #[doc(hidden)]
    pub fn metrics(&self) -> &lsm_tree::Metrics {
        &**self.tree.metrics()
    }

    /// Returns the underlying LSM-tree's path.
    #[must_use]
    pub fn path(&self) -> &Path {
        self.tree.tree_config().path.as_path()
    }

    /// Returns the disk space usage of this keyspace.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Database, KeyspaceCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = Database::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
    /// assert_eq!(0, tree.disk_space());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    #[must_use]
    pub fn disk_space(&self) -> u64 {
        self.tree.disk_space()
    }

    /// Returns an iterator that scans through the entire keyspace.
    ///
    /// # Caution
    ///
    /// Avoid using this function, or limit it as otherwise it may scan a lot of items.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Database, KeyspaceCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = Database::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
    /// tree.insert("a", "abc")?;
    /// tree.insert("f", "abc")?;
    /// tree.insert("g", "abc")?;
    /// assert_eq!(3, tree.iter().count());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    #[must_use]
    #[expect(clippy::iter_without_into_iter)]
    pub fn iter(&self) -> Iter {
        let nonce = self.supervisor.snapshot_tracker.open();
        let iter = self.tree.iter(nonce.instant, None);
        crate::iter::Iter::new(nonce, iter)
    }

    /// Returns an iterator over a range of items.
    ///
    /// # Caution
    ///
    /// Avoid using full or unbounded ranges as they may scan a lot of items (unless limited).
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Database, KeyspaceCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = Database::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
    /// tree.insert("a", "abc")?;
    /// tree.insert("f", "abc")?;
    /// tree.insert("g", "abc")?;
    /// assert_eq!(2, tree.range("a"..="f").count());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    pub fn range<K: AsRef<[u8]>, R: RangeBounds<K>>(&self, range: R) -> Iter {
        let nonce = self.supervisor.snapshot_tracker.open();
        let iter = self.tree.range(range, SeqNo::MAX, None);
        crate::iter::Iter::new(nonce, iter)
    }

    /// Returns an iterator over a prefixed set of items.
    ///
    /// # Caution
    ///
    /// Avoid using an empty prefix as it may scan a lot of items (unless limited).
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Database, KeyspaceCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = Database::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
    /// tree.insert("a", "abc")?;
    /// tree.insert("ab", "abc")?;
    /// tree.insert("abc", "abc")?;
    /// assert_eq!(2, tree.prefix("ab").count());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    pub fn prefix<K: AsRef<[u8]>>(&self, prefix: K) -> Iter {
        let nonce = self.supervisor.snapshot_tracker.open();
        let iter = self.tree.prefix(prefix, SeqNo::MAX, None);
        crate::iter::Iter::new(nonce, iter)
    }

    /// Approximates the number of items in the keyspace.
    ///
    /// For update- or delete-heavy workloads, this value will
    /// diverge from the real value, but is a O(1) operation.
    ///
    /// For insert-only workloads (e.g. logs, time series)
    /// this value is reliable.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Database, KeyspaceCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = Database::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
    /// assert_eq!(tree.approximate_len(), 0);
    ///
    /// tree.insert("1", "abc")?;
    /// assert_eq!(tree.approximate_len(), 1);
    ///
    /// tree.remove("1")?;
    /// // Oops! approximate_len will not be reliable here
    /// assert_eq!(tree.approximate_len(), 2);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    #[must_use]
    pub fn approximate_len(&self) -> usize {
        self.tree.approximate_len()
    }

    /// Scans the entire keyspace, returning the number of items.
    ///
    /// # Caution
    ///
    /// This operation scans the entire keyspace: O(n) complexity!
    ///
    /// Never, under any circumstances, use .`len()` == 0 to check
    /// if the keyspace is empty, use [`Keyspace::is_empty`] instead.
    ///
    /// If you want an estimate, use [`Keyspace::approximate_len`] instead,
    /// which is a O(1) operation.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Database, KeyspaceCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = Database::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
    /// assert_eq!(tree.len()?, 0);
    ///
    /// tree.insert("1", "abc")?;
    /// tree.insert("3", "abc")?;
    /// tree.insert("5", "abc")?;
    /// assert_eq!(tree.len()?, 3);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn len(&self) -> crate::Result<usize> {
        let mut count = 0;

        for guard in self.iter() {
            let _ = guard.key()?;
            count += 1;
        }

        Ok(count)
    }

    /// Returns `true` if the keyspace is empty.
    ///
    /// This operation has O(log N) complexity.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Database, KeyspaceCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = Database::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
    /// assert!(tree.is_empty()?);
    ///
    /// tree.insert("a", "abc")?;
    /// assert!(!tree.is_empty()?);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn is_empty(&self) -> crate::Result<bool> {
        self.tree.is_empty(SeqNo::MAX, None).map_err(Into::into)
    }

    /// Returns `true` if the keyspace contains the specified key.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Database, KeyspaceCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = Database::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
    /// assert!(!tree.contains_key("a")?);
    ///
    /// tree.insert("a", "abc")?;
    /// assert!(tree.contains_key("a")?);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn contains_key<K: AsRef<[u8]>>(&self, key: K) -> crate::Result<bool> {
        self.tree.contains_key(key, SeqNo::MAX).map_err(Into::into)
    }

    /// Retrieves an item from the keyspace.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Database, KeyspaceCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = Database::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
    /// tree.insert("a", "my_value")?;
    ///
    /// let item = tree.get("a")?;
    /// assert_eq!(Some("my_value".as_bytes().into()), item);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> crate::Result<Option<lsm_tree::UserValue>> {
        Ok(self.tree.get(key, SeqNo::MAX)?)
    }

    /// Retrieves the size of an item from the keyspace.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Database, KeyspaceCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = Database::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
    /// tree.insert("a", "my_value")?;
    ///
    /// let len = tree.size_of("a")?.unwrap_or_default();
    /// assert_eq!("my_value".len() as u32, len);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn size_of<K: AsRef<[u8]>>(&self, key: K) -> crate::Result<Option<u32>> {
        Ok(self.tree.size_of(key, SeqNo::MAX)?)
    }

    /// Returns the first key-value pair in the keyspace.
    /// The key in this pair is the minimum key in the keyspace.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Database, KeyspaceCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = Database::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
    /// tree.insert("1", "abc")?;
    /// tree.insert("3", "abc")?;
    /// tree.insert("5", "abc")?;
    ///
    /// let key = tree.first_key_value().expect("item should exist").key()?;
    /// assert_eq!(&*key, "1".as_bytes());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    pub fn first_key_value(&self) -> Option<Guard> {
        self.tree.first_key_value(SeqNo::MAX, None).map(Guard)
    }

    /// Returns the last key-value pair in the keyspace.
    /// The key in this pair is the maximum key in the keyspace.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Database, KeyspaceCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = Database::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
    /// tree.insert("1", "abc")?;
    /// tree.insert("3", "abc")?;
    /// tree.insert("5", "abc")?;
    ///
    /// let key = tree.last_key_value().expect("item should exist").key()?;
    /// assert_eq!(&*key, "5".as_bytes());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    pub fn last_key_value(&self) -> Option<Guard> {
        self.tree.last_key_value(SeqNo::MAX, None).map(Guard)
    }

    /// Returns `true` if the underlying LSM-tree is key-value-separated.
    #[must_use]
    pub fn is_kv_separated(&self) -> bool {
        matches!(self.tree, crate::AnyTree::Blob(_))
    }

    /// Flushes all sealed memtables into tables, sealing the active memtable
    /// first if `rotate` is set.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    #[doc(hidden)]
    pub fn flush(&self, rotate: bool) -> crate::Result<()> {
        if rotate {
            let mut ledger = self.supervisor.ledger()?;

            // IMPORTANT: Check the poisoned flag after getting the ledger lock, otherwise TOCTOU
            if self.is_poisoned.is_poisoned() {
                return Err(crate::Error::Poisoned);
            }

            // NOTE: A deletion (or clear) racing with a flush is fine,
            // because the flush is then discarded, see `Keyspace::install_flush`
            if self.is_deleted.load(std::sync::atomic::Ordering::Acquire) {
                return Err(crate::Error::KeyspaceDeleted);
            }

            // NOTE: Not queueing a background flush, because it is flushed right below
            self.seal_active_memtable(&mut ledger, self.tree.active_memtable().id())?;
        }

        let flush_result = {
            // NOTE: Only the tables are written under the flush lock, not the install
            //
            // IMPORTANT: Released before taking the ledger lock
            // (lock order: ledger lock, keyspaces lock, flush lock)
            let _flush_lock = self.tree.get_flush_lock();

            let gc_watermark = self.supervisor.snapshot_tracker.get_seqno_safe_to_gc();
            self.tree.flush(gc_watermark)?
        };

        let Some(flush_result) = flush_result else {
            return Ok(());
        };

        let mut ledger = self.supervisor.ledger()?;

        // IMPORTANT: Check the poisoned flag after getting the ledger lock, otherwise TOCTOU
        if self.is_poisoned.is_poisoned() {
            return Err(crate::Error::Poisoned);
        }

        // NOTE: A deletion (or clear) racing with a flush is fine,
        // because the flush is then discarded, see `Keyspace::install_flush`
        if self.is_deleted.load(std::sync::atomic::Ordering::Acquire) {
            return Err(crate::Error::KeyspaceDeleted);
        }

        // NOTE: Discarded if some of its sealed memtables are gone: the keyspace was cleared
        // or deleted, or another flush (e.g. `flush_all`, an ingestion) installed them first
        if !self.install_flush(&mut ledger, flush_result)? {
            log::trace!("Flush of keyspace {:?} was discarded", self.name);
            return Ok(());
        }

        self.supervisor
            .maybe_rotate_manifest(&mut ledger, &self.is_poisoned)?;

        self.supervisor.prune_wal(&mut ledger)
    }

    /// Seals the active memtable, and flushes all sealed memtables.
    ///
    /// Unlike [`Keyspace::flush`], the caller holds the ledger lock for the whole flush
    /// (e.g. `flush_all`, ingestion), so writes are blocked meanwhile.
    ///
    /// IMPORTANT: The caller must hold the tree's flush lock.
    pub(crate) fn flush_locked(
        &self,
        ledger: &mut crate::ledger::LedgerGuard<'_>,
    ) -> crate::Result<()> {
        // NOTE: Writes are applied to the active memtable under the ledger lock,
        // so no write can land in the memtable after it was sealed
        self.version_set_provider.rotate_active_memtable(&self.tree);

        let gc_watermark = self.supervisor.snapshot_tracker.get_seqno_safe_to_gc();

        if let Some(flush_result) = self.tree.flush(gc_watermark)? {
            self.install_flush(ledger, flush_result)?;
        }

        Ok(())
    }

    /// Persists and installs a flush result.
    ///
    /// Returns `false` if the flush was discarded, because some of its sealed memtables
    /// do not exist anymore (the keyspace was cleared or deleted, or another flush
    /// installed them first).
    pub(crate) fn install_flush(
        &self,
        ledger: &mut crate::ledger::LedgerGuard<'_>,
        flush_result: lsm_tree::FlushToTablesResult,
    ) -> crate::Result<bool> {
        // IMPORTANT: https://github.com/fjall-rs/fjall/issues/329
        ledger.wal.persist(Some(crate::DurabilityLevel::SyncAll))?;

        let seqno = self.supervisor.seqno.next();

        let flushed_bytes = self
            .version_set_provider
            .register_flush(&mut ledger.persister(self.id), flush_result, seqno)
            .inspect_err(|e| {
                log::error!("Installing a new version failed, which is a FATAL failure: {e:?}");
                self.is_poisoned.poison();
            })?;

        self.supervisor.write_buffer_size.free(flushed_bytes);

        // NOTE: Published even if discarded, the seqno was handed out
        self.supervisor.snapshot_tracker.publish(seqno);

        self.prune_versions();

        Ok(flushed_bytes > 0)
    }

    /// Seals the active memtable, without flushing it.
    ///
    /// Returns `false` if the active memtable was empty.
    ///
    /// # Errors
    ///
    /// Will return `Err` if the keyspace is poisoned.
    #[doc(hidden)]
    pub fn rotate_memtable(&self) -> crate::Result<bool> {
        let ledger = self.supervisor.ledger()?;
        let memtable_id = self.tree.active_memtable().id();
        self.inner_rotate_memtable(ledger, memtable_id)
    }

    /// Seals the active memtable (if it is still `memtable_id`) and queues a flush.
    ///
    /// Returns `false` if the memtable was already rotated by someone else, or is empty.
    pub(crate) fn inner_rotate_memtable(
        &self,
        mut ledger: crate::ledger::LedgerGuard<'_>,
        memtable_id: lsm_tree::MemtableId,
    ) -> crate::Result<bool> {
        if !self.seal_active_memtable(&mut ledger, memtable_id)? {
            return Ok(false);
        }

        drop(ledger);

        self.request_flush();

        // NOTE: The rotation replaced the latest version, so drop old versions no snapshot needs
        self.prune_versions();

        Ok(true)
    }

    /// Seals the active memtable (if it is still `memtable_id`), without flushing it.
    ///
    /// Returns `false` if the memtable was already rotated by someone else, or is empty
    /// (or the keyspace is deleted).
    ///
    /// Also rotates the WAL if its active fragment is over the rotation threshold.
    ///
    /// NOTE: Takes the ledger guard, because writers load the active memtable and insert
    /// into it under the ledger lock, so no write can land in the memtable after sealing.
    fn seal_active_memtable(
        &self,
        ledger: &mut crate::ledger::LedgerGuard<'_>,
        memtable_id: lsm_tree::MemtableId,
    ) -> crate::Result<bool> {
        log::debug!("Rotating keyspace {:?}", self.name);

        // IMPORTANT: Check the poisoned flag after getting the ledger lock, otherwise TOCTOU
        if self.is_poisoned.is_poisoned() {
            return Err(crate::Error::Poisoned);
        }

        // NOTE: The deletion holds the ledger lock, so this can't race with it
        if self.is_deleted.load(std::sync::atomic::Ordering::Acquire) {
            return Ok(false);
        }

        // NOTE: The rotation request may be stale (someone else already rotated)
        if self.tree.active_memtable().id() != memtable_id {
            log::debug!("Active memtable of {:?} was already rotated", self.name);
            return Ok(false);
        }

        if self
            .version_set_provider
            .rotate_active_memtable(&self.tree)
            .is_none()
        {
            log::debug!(
                "Got no sealed memtable for {:?}, nothing to rotate",
                self.name
            );
            return Ok(false);
        }

        self.supervisor.maybe_rotate_wal(ledger)?;

        Ok(true)
    }

    fn check_write_halt(&self) {
        while self.tree.l0_run_count() >= 30 {
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    pub(crate) fn local_backpressure(&self) -> bool {
        if self.supervisor.db_config.disable_write_stalls {
            return false;
        }

        let mut throttled = false;

        let l0_run_count = self.tree.l0_run_count();

        if l0_run_count >= 20 {
            perform_write_stall(l0_run_count);
            self.check_write_halt();
            throttled = true;
        }

        while self.tree.sealed_memtable_count() >= 4 {
            log::debug!(
                "Halting writes because we have 4+ sealed memtables in {:?} queued up",
                self.name,
            );
            std::thread::sleep(Duration::from_millis(100));
            throttled = true;
        }

        throttled
    }

    /// Queues a background flush of the sealed memtables.
    fn request_flush(&self) {
        self.worker_messager
            .send(WorkerMessage::Flush(self.clone()))
            .ok();
    }

    pub(crate) fn request_rotation(&self) {
        use lsm_tree::AbstractTree;

        let latest_super_version = self.tree.current_super_version();
        let active_memtable = &latest_super_version.active_memtable;

        self.worker_messager
            .try_send(WorkerMessage::RotateMemtable(
                self.clone(),
                active_memtable.id(),
            ))
            .ok();
    }

    pub(crate) fn check_memtable_rotate(&self, size: u64) {
        if size > self.config.max_memtable_size {
            self.request_rotation();
        }
    }

    fn maintenance(&self, memtable_size: u64) {
        self.check_memtable_rotate(memtable_size);
        self.local_backpressure();
    }

    #[doc(hidden)]
    #[must_use]
    pub fn l0_table_count(&self) -> usize {
        self.tree.level_table_count(0).unwrap_or_default()
    }

    /// Number of tables (a.k.a. SST files) in the LSM-tree.
    #[doc(hidden)]
    #[must_use]
    pub fn table_count(&self) -> usize {
        self.tree.table_count()
    }

    /// Number of blob files in the LSM-tree.
    #[doc(hidden)]
    #[must_use]
    pub fn blob_file_count(&self) -> usize {
        self.tree.blob_file_count()
    }

    /// Runs a single compaction with the configured compaction strategy,
    /// blocking the caller until it's done.
    ///
    /// Does not involve the background workers.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    #[doc(hidden)]
    pub fn compact(&self) -> crate::Result<()> {
        crate::compaction::worker::compact_with(self, self.config.compaction_strategy.clone())
    }

    /// Performs major compaction, blocking the caller until it's done.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    #[doc(hidden)]
    pub fn major_compact(&self) -> crate::Result<()> {
        // TODO: v4 ----v
        //   compaction strategy needs a method: strategy.table_target_size()?
        // NOTE: Exclusive, so two major compactions don't run at the same time
        crate::compaction::worker::compact_exclusive(
            self,
            Arc::new(lsm_tree::compaction::Major::new(
                64 * 1_024 * 1_024, // 64 MiB
            )),
        )
    }

    /// Inserts a key-value pair into the keyspace.
    ///
    /// Keys may be up to 65536 bytes long, values up to 2^32 bytes.
    /// Shorter keys and values result in better performance.
    ///
    /// If the key already exists, the item will be overwritten.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Database, KeyspaceCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = Database::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
    /// tree.insert("a", "abc")?;
    ///
    /// assert!(!tree.is_empty()?);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn insert<K: Into<UserKey>, V: Into<UserValue>>(
        &self,
        key: K,
        value: V,
        durability: DurabilityLevel,
    ) -> crate::Result<()> {
        use std::sync::atomic::Ordering::Relaxed;

        if self.is_deleted.load(Relaxed) {
            return Err(crate::Error::KeyspaceDeleted);
        }

        let key = key.into();
        let value = value.into();

        let write_record = WriteRecord::new(WriteRecordPayload::Single(
            crate::batch::item::Item::new(self.clone(), key, value, lsm_tree::ValueType::Value),
        ))
        .durability(durability);

        #[expect(
            clippy::expect_used,
            reason = "does not use OCC, so should not conflict"
        )]
        #[expect(clippy::missing_panics_doc)]
        self.supervisor
            .write_pipeline
            .commit(Arc::new(write_record))?
            .expect("should not do OCC");

        Ok(())
    }

    /// Removes an item from the keyspace.
    ///
    /// The key may be up to 65536 bytes long.
    /// Shorter keys result in better performance.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Database, KeyspaceCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = Database::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
    /// tree.insert("a", "abc")?;
    ///
    /// let item = tree.get("a")?.expect("should have item");
    /// assert_eq!("abc".as_bytes(), &*item);
    ///
    /// tree.remove("a")?;
    ///
    /// let item = tree.get("a")?;
    /// assert_eq!(None, item);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn remove<K: Into<UserKey>>(
        &self,
        key: K,
        durability: DurabilityLevel,
    ) -> crate::Result<()> {
        use std::sync::atomic::Ordering::Relaxed;

        if self.is_deleted.load(Relaxed) {
            return Err(crate::Error::KeyspaceDeleted);
        }

        let key = key.into();

        let write_record = WriteRecord::new(WriteRecordPayload::Single(
            crate::batch::item::Item::new_tombstone(self.clone(), key, false),
        ))
        .durability(durability);

        #[expect(
            clippy::expect_used,
            reason = "does not use OCC, so should not conflict"
        )]
        #[expect(clippy::missing_panics_doc)]
        self.supervisor
            .write_pipeline
            .commit(Arc::new(write_record))?
            .expect("should not do OCC");

        Ok(())
    }

    /// Removes an item from the keyspace, leaving behind a weak tombstone.
    ///
    /// When a weak tombstone is matched with a single write in a compaction,
    /// the tombstone will be removed along with the value. If the key was
    /// overwritten the result of a `remove_weak` is undefined.
    ///
    /// Only use this remove if it is known that the key has only been written
    /// to once since its creation or last `remove_weak`.
    ///
    /// The key may be up to 65536 bytes long.
    /// Shorter keys result in better performance.
    ///
    /// # Experimental
    ///
    /// This function is currently experimental.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Database, KeyspaceCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = Database::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
    /// tree.insert("a", "abc")?;
    ///
    /// let item = tree.get("a")?.expect("should have item");
    /// assert_eq!("abc".as_bytes(), &*item);
    ///
    /// tree.remove_weak("a")?;
    ///
    /// let item = tree.get("a")?;
    /// assert_eq!(None, item);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    #[doc(hidden)]
    pub fn remove_weak<K: Into<UserKey>>(
        &self,
        key: K,
        durability: DurabilityLevel,
    ) -> crate::Result<()> {
        use std::sync::atomic::Ordering::Relaxed;

        if self.is_deleted.load(Relaxed) {
            return Err(crate::Error::KeyspaceDeleted);
        }

        let key = key.into();

        let write_record = WriteRecord::new(WriteRecordPayload::Single(
            crate::batch::item::Item::new_tombstone(self.clone(), key, true),
        ))
        .durability(durability);

        #[expect(
            clippy::expect_used,
            reason = "does not use OCC, so should not conflict"
        )]
        self.supervisor
            .write_pipeline
            .commit(Arc::new(write_record))?
            .expect("should not do OCC");

        Ok(())
    }
}
