// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

mod config;
pub mod name;
pub mod options;
mod write_delay;

use crate::{
    db::Keyspaces,
    db_config::Config as DatabaseConfig,
    file::{KEYSPACES_FOLDER, LSM_MANIFEST_FILE},
    flush::Task as FlushTask,
    journal::{
        manager::{EvictionWatermark, JournalManager},
        Journal,
    },
    locked_file::LockedFileGuard,
    stats::Stats,
    supervisor::Supervisor,
    worker_pool::{WorkerMessage, WorkerPool},
    Database, Guard,
};
use lsm_tree::{AbstractTree, AnyTree, KvPair, SeqNo, SequenceNumberCounter, UserKey, UserValue};
use options::CreateOptions;
use std::{
    ops::RangeBounds,
    path::Path,
    sync::{
        atomic::{AtomicBool, AtomicUsize},
        Arc, RwLock,
    },
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
}

#[allow(clippy::module_name_repetitions)]
pub struct KeyspaceInner {
    /// Internal ID
    pub(crate) id: InternalKeyspaceId,

    // Internal
    //
    /// Keyspace name
    pub name: KeyspaceKey,

    // Keyspace configuration
    #[doc(hidden)]
    pub config: CreateOptions,

    /// If `true`, the keyspace is marked as deleted
    pub(crate) is_deleted: AtomicBool,

    /// If `true`, fsync failed during persisting, see `Error::Poisoned`
    pub(crate) is_poisoned: Arc<AtomicBool>,

    /// LSM-tree wrapper
    #[doc(hidden)]
    pub tree: AnyTree,

    // Database stuff
    //
    /// Config of database
    pub(crate) db_config: DatabaseConfig,

    supervisor: Supervisor,

    /// Journal of database
    pub(crate) journal: Arc<Journal>,

    /// Keyspace map of database
    pub(crate) keyspaces: Arc<RwLock<Keyspaces>>,

    /// Visible sequence number of database
    #[doc(hidden)]
    pub visible_seqno: SequenceNumberCounter,

    /// Database-level stats
    pub(crate) stats: Arc<Stats>,

    /// Number of completed memtable flushes in this keyspace
    pub(crate) flushes_completed: AtomicUsize,

    pub(crate) worker_pool: WorkerPool, // TODO: 3.0.0 remove?
    pub(crate) worker_messager: flume::Sender<WorkerMessage>,

    lock_file: LockedFileGuard,
}

impl Drop for KeyspaceInner {
    fn drop(&mut self) {
        log::trace!("Dropping keyspace inner: {:?}", self.name);

        if self.is_deleted.load(std::sync::atomic::Ordering::Acquire) {
            let path = &self.tree.tree_config().path;

            // IMPORTANT: First, delete the manifest,
            // once that is deleted, the keyspace is treated as uninitialized
            // even if the .deleted marker is removed
            //
            // This is important, because if somehow `remove_dir_all` ends up
            // deleting the `.deleted` marker first, we would end up resurrecting
            // the keyspace
            let manifest_file = path.join(LSM_MANIFEST_FILE);

            // TODO: use https://github.com/rust-lang/rust/issues/31436 if stable
            #[allow(clippy::collapsible_else_if)]
            match manifest_file.try_exists() {
                Ok(exists) => {
                    if exists {
                        if let Err(e) = std::fs::remove_file(manifest_file) {
                            log::error!(
                                "Failed to cleanup keyspace manifest at {}: {e}",
                                path.display(),
                            );
                        } else {
                            if let Err(e) = std::fs::remove_dir_all(path) {
                                log::error!(
                                    "Failed to cleanup deleted keyspace's folder at {}: {e}",
                                    path.display(),
                                );
                            }
                        }
                    }
                }
                Err(e) => {
                    log::error!(
                        "Failed to cleanup keyspace manifest at {}: {e}",
                        path.display(),
                    );
                }
            }
        }

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
#[allow(clippy::module_name_repetitions)]
#[doc(alias = "column family")]
#[doc(alias = "locality group")]
#[doc(alias = "table")]
pub struct Keyspace(pub(crate) Arc<KeyspaceInner>);

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
    /// Returns the number of blob bytes on disk that are not referenced.
    ///
    /// These will be reclaimed over time by blob garbage collection automatically.
    #[must_use]
    pub fn fragmented_blob_bytes(&self) -> u64 {
        self.tree.stale_blob_bytes()
    }

    /// Ingests a sorted stream of key-value pairs into the keyspace.
    ///
    /// Can only be called on a new fresh, empty keyspace.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    ///
    /// # Panics
    ///
    /// Panics if the keyspace is **not** initially empty.
    ///
    /// Panics if the input iterator is not sorted in ascending order.
    pub fn ingest<K: Into<UserKey>, V: Into<UserValue>>(
        &self,
        iter: impl Iterator<Item = (K, V)>,
    ) -> crate::Result<()> {
        self.tree
            .ingest(
                iter.map(|(k, v)| (k.into(), v.into())),
                self.supervisor.snapshot_tracker.seqno_ref(),
                &self.visible_seqno,
            )
            .inspect(|()| {
                self.worker_messager
                    .try_send(WorkerMessage::Compact(self.clone()))
                    .ok();
            })
            .map_err(Into::into)
    }

    pub(crate) fn from_database(
        keyspace_id: InternalKeyspaceId,
        db: &Database,
        tree: AnyTree,
        name: KeyspaceKey,
        config: CreateOptions,
    ) -> Self {
        Self(Arc::new(KeyspaceInner {
            supervisor: db.supervisor.clone(),
            worker_pool: db.worker_pool.clone(),
            worker_messager: db.worker_messager.clone(),
            id: keyspace_id,
            name,
            tree,
            keyspaces: db.keyspaces.clone(),
            db_config: db.config.clone(),
            flushes_completed: AtomicUsize::new(0),
            journal: db.journal.clone(),
            visible_seqno: db.visible_seqno.clone(),
            is_deleted: AtomicBool::default(),
            is_poisoned: db.is_poisoned.clone(),
            config,
            stats: db.stats.clone(),
            lock_file: db.lock_file.clone(),
        }))
    }

    /// Creates a new keyspace.
    pub(crate) fn create_new(
        keyspace_id: InternalKeyspaceId,
        db: &Database,
        name: KeyspaceKey,
        config: CreateOptions,
    ) -> crate::Result<Self> {
        log::debug!("Creating keyspace {name:?}->{keyspace_id}");

        let base_folder = db
            .config
            .path
            .join(KEYSPACES_FOLDER)
            .join(keyspace_id.to_string());

        std::fs::create_dir_all(&base_folder)?;

        let base_config = lsm_tree::Config::new(
            base_folder,
            db.supervisor.snapshot_tracker.seqno_ref().clone(),
        )
        .use_descriptor_table(db.config.descriptor_table.clone())
        .use_cache(db.config.cache.clone());

        let base_config = apply_to_base_config(base_config, &config);
        let tree = base_config.open()?;

        Ok(Self(Arc::new(KeyspaceInner {
            supervisor: db.supervisor.clone(),
            worker_pool: db.worker_pool.clone(),
            worker_messager: db.worker_messager.clone(),
            id: keyspace_id,
            name,
            config,
            keyspaces: db.keyspaces.clone(),
            db_config: db.config.clone(),
            flushes_completed: AtomicUsize::new(0),
            journal: db.journal.clone(),
            visible_seqno: db.visible_seqno.clone(),
            tree,
            is_deleted: AtomicBool::default(),
            is_poisoned: db.is_poisoned.clone(),
            stats: db.stats.clone(),
            lock_file: db.lock_file.clone(),
        })))
    }

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
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
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
    /// Avoid using this function, or limit it as otherwise it may scan a lot of items.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Database, KeyspaceCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = Database::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
    /// tree.insert("a", "abc")?;
    /// tree.insert("f", "abc")?;
    /// tree.insert("g", "abc")?;
    /// assert_eq!(3, tree.iter().count());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    #[must_use]
    pub fn iter(&self) -> impl DoubleEndedIterator<Item = Guard> + '_ {
        let nonce = self.supervisor.snapshot_tracker.open();
        let iter = self.tree.iter(nonce.instant, None).map(Guard);
        crate::iter::Iter::new(nonce, iter)
    }

    /// Returns an iterator over a range of items.
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
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
    /// tree.insert("a", "abc")?;
    /// tree.insert("f", "abc")?;
    /// tree.insert("g", "abc")?;
    /// assert_eq!(2, tree.range("a"..="f").count());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    pub fn range<'a, K: AsRef<[u8]> + 'a, R: RangeBounds<K> + 'a>(
        &'a self,
        range: R,
    ) -> impl DoubleEndedIterator<Item = Guard> + 'a {
        let nonce = self.supervisor.snapshot_tracker.open();
        let iter = self.tree.range(range, SeqNo::MAX, None).map(Guard);
        crate::iter::Iter::new(nonce, iter)
    }

    /// Returns an iterator over a prefixed set of items.
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
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
    /// tree.insert("a", "abc")?;
    /// tree.insert("ab", "abc")?;
    /// tree.insert("abc", "abc")?;
    /// assert_eq!(2, tree.prefix("ab").count());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    pub fn prefix<'a, K: AsRef<[u8]> + 'a>(
        &'a self,
        prefix: K,
    ) -> impl DoubleEndedIterator<Item = Guard> + 'a {
        let nonce = self.supervisor.snapshot_tracker.open();
        let iter = self.tree.prefix(prefix, SeqNo::MAX, None).map(Guard);
        crate::iter::Iter::new(nonce, iter)
    }

    /// Approximates the amount of items in the keyspace.
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
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
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

    /// Scans the entire keyspace, returning the amount of items.
    ///
    /// ###### Caution
    ///
    /// This operation scans the entire keyspace: O(n) complexity!
    ///
    /// Never, under any circumstances, use .`len()` == 0 to check
    /// if the keyspace is empty, use [`Keyspace::is_empty`] instead.
    ///
    /// If you want an estimate, use [`Keyspace::approximate_len`] instead.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Database, KeyspaceCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = Database::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
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
            let _ = guard.key();
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
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
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
        self.first_key_value().map(|x| x.is_none())
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
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
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
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
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
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
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
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
    /// tree.insert("1", "abc")?;
    /// tree.insert("3", "abc")?;
    /// tree.insert("5", "abc")?;
    ///
    /// let (key, _) = tree.first_key_value()?.expect("item should exist");
    /// assert_eq!(&*key, "1".as_bytes());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn first_key_value(&self) -> crate::Result<Option<KvPair>> {
        Ok(self.tree.first_key_value(SeqNo::MAX, None)?)
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
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
    /// tree.insert("1", "abc")?;
    /// tree.insert("3", "abc")?;
    /// tree.insert("5", "abc")?;
    ///
    /// let (key, _) = tree.last_key_value()?.expect("item should exist");
    /// assert_eq!(&*key, "5".as_bytes());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn last_key_value(&self) -> crate::Result<Option<KvPair>> {
        Ok(self.tree.last_key_value(SeqNo::MAX, None)?)
    }

    /// Returns `true` if the underlying LSM-tree is key-value-separated.
    #[must_use]
    pub fn is_kv_separated(&self) -> bool {
        matches!(self.tree, crate::AnyTree::Blob(_))
    }

    // NOTE: Used in tests
    #[doc(hidden)]
    pub fn rotate_memtable_and_wait(&self) -> crate::Result<()> {
        let watermark = self.flushes_completed();

        if self.rotate_memtable()? {
            self.supervisor.flush_manager.wait_for_empty();

            while self.flushes_completed() <= watermark {}
        }

        Ok(())
    }

    /// Returns `true` if the memtable was indeed rotated.
    #[doc(hidden)]
    pub fn rotate_memtable(&self) -> crate::Result<bool> {
        log::debug!("Rotating memtable {:?}", self.name);

        log::trace!("keyspace: acquiring journal lock");
        let mut journal = self.journal.get_writer();

        // Rotate memtable
        let Some(_) = self.tree.rotate_memtable() else {
            log::debug!("Got no sealed memtable, someone beat us to it");
            return Ok(false);
        };

        log::trace!("keyspace: acquiring journal manager lock");
        let mut journal_manager: std::sync::RwLockWriteGuard<'_, JournalManager> = self
            .supervisor
            .journal_manager
            .write()
            .expect("lock is poisoned");

        let seqno_map = {
            let keyspaces = self.keyspaces.write().expect("lock is poisoned");

            let mut seqnos = Vec::with_capacity(keyspaces.len());

            for keyspace in keyspaces.values() {
                if let Some(lsn) = keyspace.tree.get_highest_memtable_seqno() {
                    seqnos.push(EvictionWatermark {
                        lsn,
                        keyspace: keyspace.clone(),
                    });
                }
            }

            seqnos
        };

        journal_manager.rotate_journal(&mut journal, seqno_map)?;

        drop(journal_manager);
        drop(journal);

        self.supervisor.flush_manager.enqueue(Arc::new(FlushTask {
            keyspace: self.clone(),
        }));

        // TODO: 3.0.0 let supervisor handle this
        self.stats
            .flushes_enqueued
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        self.worker_messager.send(WorkerMessage::Flush).ok();

        // TODO: 3.0.0 dirty monkey patch
        // TODO: we need a mechanism to prevent the version free list from getting too large
        // TODO: in a write only workload
        {
            let current_seqno = self.supervisor.snapshot_tracker.get();
            let gc_seqno_watermark = self.supervisor.snapshot_tracker.get_seqno_safe_to_gc();

            // NOTE: If the difference between watermark is too large, and
            // we never opened a snapshot, we need to pull the watermark up
            //
            // https://github.com/fjall-rs/fjall/discussions/85
            if (current_seqno - gc_seqno_watermark) > 100
                && self.supervisor.snapshot_tracker.len() == 0
            {
                self.supervisor.snapshot_tracker.pullup();
            }

            self.supervisor.snapshot_tracker.gc();
        }

        if self.tree.version_free_list_len() >= 100 {
            log::warn!(
                "The version free list has grown very large ({}) - maybe you are keeping a snapshot/read transaction open for too long?", 
                self.tree.version_free_list_len(),
            );
        }

        Ok(true)
    }

    fn check_write_stall(&self) {
        let l0_run_count = self.tree.l0_run_count();

        if l0_run_count >= 20 {
            perform_write_stall(l0_run_count);
            self.check_write_halt();
        }

        while self.tree.sealed_memtable_count() >= 4 {
            log::debug!(
                "Halting writes because we have 4+ sealed memtables in {:?} queued up",
                self.name,
            );
            std::thread::sleep(Duration::from_millis(100));
        }

        {
            let start = std::time::Instant::now();

            while {
                self.supervisor
                    .journal_manager
                    .read()
                    .expect("lock is poisoned")
                    .disk_space_used()
            } >= self.db_config.max_journaling_size_in_bytes
            {
                std::thread::sleep(std::time::Duration::from_millis(10));

                if start.elapsed() > std::time::Duration::from_secs(5) {
                    log::debug!(
                        "Halting writes for 5+ secs now because journal is still too large"
                    );

                    // // TODO: this may not scale well for many keyspaces
                    {
                        let keyspaces = self.keyspaces.read().expect("lock is poisoned");

                        let mut keyspaces_with_seqno = keyspaces
                            .values()
                            .filter(|x| x.tree.active_memtable_size() > 0)
                            .map(|x| (x.clone(), x.tree.get_highest_persisted_seqno()))
                            .collect::<Vec<_>>();

                        drop(keyspaces);

                        keyspaces_with_seqno.sort_by(|a, b| a.1.cmp(&b.1));

                        if let Some(lowest) = keyspaces_with_seqno.first() {
                            log::debug!(
                                "Rotating {:?} to try to reduce journal size",
                                lowest.0.name,
                            );

                            match lowest.0.rotate_memtable() {
                                Ok(_) => {
                                    self.supervisor.flush_manager.enqueue(Arc::new(FlushTask {
                                        keyspace: lowest.0.clone(),
                                    }));
                                    self.worker_messager.try_send(WorkerMessage::Flush).ok();
                                }
                                Err(e) => {
                                    log::warn!(
                                        "Rotating keyspace {:?} failed: {e:?}",
                                        lowest.0.name,
                                    );
                                }
                            }
                        }
                    }

                    std::thread::sleep(Duration::from_millis(490));
                }

                if self.is_poisoned.load(std::sync::atomic::Ordering::Relaxed) {
                    log::error!("DB was poisoned while being write halted");
                    return;
                }
            }
        }

        {
            let start = std::time::Instant::now();

            while self.supervisor.write_buffer_size.get()
                >= self.db_config.max_write_buffer_size_in_bytes
            {
                std::thread::sleep(std::time::Duration::from_millis(10));

                if start.elapsed() > std::time::Duration::from_secs(3) {
                    log::debug!("Halting writes for 3+ secs now because database write buffer is still too large");

                    {
                        let keyspaces = self.keyspaces.read().expect("lock is poisoned");

                        let mut keyspaces_with_seqno = keyspaces
                            .values()
                            .filter(|x| x.tree.active_memtable_size() > 0)
                            .map(|x| (x.clone(), x.tree.active_memtable_size()))
                            .collect::<Vec<_>>();

                        drop(keyspaces);

                        keyspaces_with_seqno.sort_by(|a, b| a.1.cmp(&b.1));

                        if let Some(lowest) = keyspaces_with_seqno.last() {
                            log::debug!(
                                "Rotating {:?} to try to reduce database write buffer size",
                                lowest.0.name,
                            );

                            match lowest.0.rotate_memtable() {
                                Ok(_) => {
                                    self.supervisor.flush_manager.enqueue(Arc::new(FlushTask {
                                        keyspace: lowest.0.clone(),
                                    }));
                                    self.worker_messager.try_send(WorkerMessage::Flush).ok();
                                }
                                Err(e) => {
                                    log::warn!(
                                        "Rotating keyspace {:?} failed: {e:?}",
                                        lowest.0.name,
                                    );
                                }
                            }
                        }
                    }

                    std::thread::sleep(Duration::from_millis(490));
                }

                if self.is_poisoned.load(std::sync::atomic::Ordering::Relaxed) {
                    log::error!("DB was poisoned while being write halted");
                    return;
                }
            }
        }
    }

    fn check_write_halt(&self) {
        let start = std::time::Instant::now();

        while self.tree.l0_run_count() >= 30 {
            std::thread::sleep(Duration::from_millis(10));

            if start.elapsed() > std::time::Duration::from_secs(5) {
                log::debug!(
                    "Halting writes for 5+ secs now because L0 of {:?} is still too full, starting to send compaction requests",
                    self.name,
                );

                self.worker_messager
                    .try_send(WorkerMessage::Compact(self.clone()))
                    .ok();

                std::thread::sleep(Duration::from_millis(490));

                if self.is_poisoned.load(std::sync::atomic::Ordering::Relaxed) {
                    log::error!("DB was poisoned while being write halted");
                    return;
                }
            }
        }
    }

    pub(crate) fn backpressure(&self, size: u64) -> crate::Result<()> {
        if size > self.config.max_memtable_size {
            self.rotate_memtable().inspect_err(|e| {
                log::error!("Memtable rotation failed: {e:?}");
                self.is_poisoned
                    .store(true, std::sync::atomic::Ordering::Relaxed);
            })?;
        }

        self.check_write_stall();

        Ok(())
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

    /// Number of completed memtable flushes in this keyspace.
    #[must_use]
    #[doc(hidden)]
    pub fn flushes_completed(&self) -> usize {
        self.flushes_completed
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Performs major compaction, blocking the caller until it's done.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    #[doc(hidden)]
    pub fn major_compact(&self) -> crate::Result<()> {
        self.tree.major_compact(
            64_000_000,
            self.supervisor.snapshot_tracker.get_seqno_safe_to_gc(),
        )?;

        // TODO: 3.0.0 ----^
        // compaction strategy needs a method: strategy.table_target_size()

        Ok(())
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
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
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
    ) -> crate::Result<()> {
        use std::sync::atomic::Ordering;

        if self.is_deleted.load(Ordering::Relaxed) {
            return Err(crate::Error::KeyspaceDeleted);
        }

        let key = key.into();
        let value = value.into();

        let mut journal_writer = self.journal.get_writer();

        // IMPORTANT: Check the poisoned flag after getting journal mutex, otherwise TOCTOU
        if self.is_poisoned.load(Ordering::Relaxed) {
            return Err(crate::Error::Poisoned);
        }

        let seqno = self.supervisor.snapshot_tracker.next();

        journal_writer.write_raw(self.id, &key, &value, lsm_tree::ValueType::Value, seqno)?;

        if !self.config.manual_journal_persist {
            journal_writer
                .persist(crate::PersistMode::Buffer)
                .map_err(|e| {
                    log::error!("persist failed, which is a FATAL, and possibly hardware-related, failure: {e:?}");
                    self.is_poisoned.store(true, Ordering::Relaxed);
                    e
                })?;
        }

        let (item_size, memtable_size) = self.tree.insert(key, value, seqno);

        self.visible_seqno.fetch_max(seqno + 1);

        drop(journal_writer);

        self.supervisor.write_buffer_size.allocate(item_size);
        self.backpressure(memtable_size)?;

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
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
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
    pub fn remove<K: Into<UserKey>>(&self, key: K) -> crate::Result<()> {
        use std::sync::atomic::Ordering;

        if self.is_deleted.load(Ordering::Relaxed) {
            return Err(crate::Error::KeyspaceDeleted);
        }

        let key = key.into();

        let mut journal_writer = self.journal.get_writer();

        // IMPORTANT: Check the poisoned flag after getting journal mutex, otherwise TOCTOU
        if self.is_poisoned.load(Ordering::Relaxed) {
            return Err(crate::Error::Poisoned);
        }

        let seqno = self.supervisor.snapshot_tracker.next();

        journal_writer.write_raw(self.id, &key, &[], lsm_tree::ValueType::Tombstone, seqno)?;

        if !self.config.manual_journal_persist {
            journal_writer
                .persist(crate::PersistMode::Buffer)
                .map_err(|e| {
                    log::error!("persist failed, which is a FATAL, and possibly hardware-related, failure: {e:?}");
                    self.is_poisoned.store(true, Ordering::Relaxed);
                    e
                })?;
        }

        let (item_size, memtable_size) = self.tree.remove(key, seqno);

        self.visible_seqno.fetch_max(seqno + 1);

        drop(journal_writer);

        self.supervisor.write_buffer_size.allocate(item_size);
        self.backpressure(memtable_size)?;

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
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
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
    pub fn remove_weak<K: Into<UserKey>>(&self, key: K) -> crate::Result<()> {
        use std::sync::atomic::Ordering;

        if self.is_deleted.load(Ordering::Relaxed) {
            return Err(crate::Error::KeyspaceDeleted);
        }

        let key = key.into();

        let mut journal_writer = self.journal.get_writer();

        // IMPORTANT: Check the poisoned flag after getting journal mutex, otherwise TOCTOU
        if self.is_poisoned.load(Ordering::Relaxed) {
            return Err(crate::Error::Poisoned);
        }

        let seqno = self.supervisor.snapshot_tracker.next();

        journal_writer.write_raw(
            self.id,
            &key,
            &[],
            lsm_tree::ValueType::WeakTombstone,
            seqno,
        )?;

        if !self.config.manual_journal_persist {
            journal_writer
                .persist(crate::PersistMode::Buffer)
                .map_err(|e| {
                    log::error!(
                        "persist failed, which is a FATAL, and possibly hardware-related, failure: {e:?}"
                    );
                    self.is_poisoned.store(true, Ordering::Relaxed);
                    e
                })?;
        }

        let (item_size, memtable_size) = self.tree.remove(key, seqno);

        self.visible_seqno.fetch_max(seqno + 1);

        drop(journal_writer);

        self.supervisor.write_buffer_size.allocate(item_size);
        self.backpressure(memtable_size)?;

        Ok(())
    }
}
