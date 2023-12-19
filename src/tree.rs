use crate::{
    compaction::CompactionStrategy,
    file::{JOURNALS_FOLDER, LSM_MARKER, PARTITIONS_FOLDER},
    get_default_partition_key,
    id::generate_segment_id,
    journal::Journal,
    partition::Partition,
    prefix::Prefix,
    range::Range,
    tree_inner::TreeInner,
    value::{SeqNo, UserData, UserKey},
    version::Version,
    Batch, Config, Snapshot, Value,
};
use std::{
    collections::HashMap,
    ops::RangeBounds,
    sync::{Arc, RwLock},
};
use std_semaphore::Semaphore;

pub struct CompareAndSwapError {
    /// The value currently in the tree that caused the CAS error
    pub prev: Option<UserData>,

    /// The value that was proposed
    pub next: Option<UserData>,
}

pub type CompareAndSwapResult = Result<(), CompareAndSwapError>;

/// A log-structured merge tree (LSM-tree/LSMT)
///
/// The tree is internally synchronized (Send + Sync), so it does not need to be wrapped in a lock nor an Arc.
///
/// To share the tree between threads, use `Arc::clone(&tree)` or `tree.clone()`.
#[derive(Clone)]
pub struct Tree(pub(crate) Arc<TreeInner>);

impl std::ops::Deref for Tree {
    type Target = Arc<TreeInner>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Tree {
    // TODO: segment_count

    /// Opens the tree at the given folder.
    ///
    /// Will create a new tree if the folder is not in use
    /// or recover a previous state if it exists.
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// # use lsm_tree::{Config, Tree};
    /// #
    /// let tree = Tree::open(Config::new(folder))?;
    /// // Same as
    /// # let folder = tempfile::tempdir()?;
    /// let tree = Config::new(folder).open()?;
    /// #
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn open(config: Config) -> crate::Result<Self> {
        log::info!("Opening LSM-tree at {}", config.path.display());

        let flush_ms = config.fsync_ms;

        let tree = if config.path.join(LSM_MARKER).exists() {
            Self::recover(config)
        } else {
            Self::create_new(config)
        };

        if let Some(ms) = flush_ms {
            if let Ok(tree) = &tree {
                tree.start_fsync_thread(ms);
            };
        }

        tree
    }

    fn start_fsync_thread(&self, ms: usize) {
        log::debug!("starting fsync thread");

        let journal = Arc::clone(&self.journal);
        let stop_signal = self.stop_signal.clone();

        std::thread::spawn(move || loop {
            log::trace!("fsync thread: sleeping {ms}ms");
            std::thread::sleep(std::time::Duration::from_millis(ms as u64));

            if stop_signal.is_stopped() {
                log::debug!("fsync thread: exiting because tree is dropping");
                return;
            }

            log::trace!("fsync thread: fsycing journal");
            if let Err(e) = journal.flush() {
                log::error!("Fsync failed: {e:?}");
            }
        });
    }

    /// Opens a read-only point-in-time snapshot of the tree
    ///
    /// Dropping the snapshot will close the snapshot
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// # use lsm_tree::{Config, Tree};
    /// #
    /// # let tree = Config::new(folder).open()?;
    /// #
    /// tree.insert("a", "abc")?;
    ///
    /// let snapshot = tree.snapshot();
    /// assert_eq!(snapshot.len()?, tree.len()?);
    ///
    /// tree.insert("b", "abc")?;
    ///
    /// assert_eq!(2, tree.len()?);
    /// assert_eq!(1, snapshot.len()?);
    ///
    /// assert!(snapshot.contains_key("a")?);
    /// assert!(!snapshot.contains_key("b")?);
    /// #
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    #[must_use]
    pub fn snapshot(&self) -> Snapshot {
        Snapshot::new(self.clone())
    }

    /// Initializes a new, atomic write batch.
    ///
    /// Call [`Batch::commit`] to commit the batch to the tree.
    ///
    /// Dropping the batch will not commit items to the tree.
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// # use lsm_tree::{Config, Tree};
    /// #
    /// # let tree = Config::new(folder).open()?;
    /// #
    /// let mut batch = tree.batch();
    /// batch.insert("a", "hello");
    /// batch.insert("b", "hello2");
    /// batch.insert("c", "hello3");
    /// batch.remove("idontlikeu");
    ///
    /// batch.commit()?;
    ///
    /// assert_eq!(3, tree.len()?);
    /// #
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    #[must_use]
    pub fn batch(&self) -> Batch {
        Batch::new(self.clone())
    }

    /// Returns `true` if there are some segments that are being compacted.
    #[doc(hidden)]
    #[must_use]
    pub fn is_compacting(&self) -> bool {
        self.partitions
            .read()
            .expect("lock is poisoned")
            .values()
            .any(Partition::is_compacting)
    }

    /// Sums the disk space usage of the tree across all partitions (segments + journals).
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// # use lsm_tree::{Config, Tree};
    /// #
    /// let tree = Config::new(folder).open()?;
    /// assert_eq!(0, tree.disk_space()?);
    /// #
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    pub fn disk_space(&self) -> crate::Result<u64> {
        let partition_size = self
            .partitions
            .read()
            .expect("lock is poisoned")
            .values()
            .map(|p| p.disk_space())
            .sum::<u64>();

        let active_journal_size = fs_extra::dir::get_size(self.config().path.join(JOURNALS_FOLDER))
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "fs_extra error"))?;

        Ok(partition_size + active_journal_size)
    }

    /// Returns the tree configuration.
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// # use lsm_tree::{Config, Tree};
    /// #
    /// let tree = Config::new(folder).open()?;
    /// assert_eq!(Config::default().block_size, tree.config().block_size);
    /// #
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    #[must_use]
    pub fn config(&self) -> Config {
        self.config.clone()
    }

    /// Scans the entire tree, returning the amount of items.
    ///
    /// ###### Caution
    ///
    /// This operation scans the entire tree: O(n) complexity!
    ///
    /// Never, under any circumstances, use .len() == 0 to check
    /// if the tree is empty, use [`Tree::is_empty`] instead.
    ///
    /// # Examples
    ///
    /// ```
    /// # use lsm_tree::Error as TreeError;
    /// # use lsm_tree::{Tree, Config};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// let tree = Config::new(folder).open()?;
    /// assert_eq!(tree.len()?, 0);
    ///
    /// tree.insert("1", "abc")?;
    /// tree.insert("3", "abc")?;
    /// tree.insert("5", "abc")?;
    /// assert_eq!(tree.len()?, 3);
    /// #
    /// # Ok::<(), TreeError>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn len(&self) -> crate::Result<usize> {
        let mut count = 0;

        for item in &self.iter() {
            let _ = item?;
            count += 1;
        }

        Ok(count)
    }

    /// Returns `true` if the tree is empty.
    ///
    /// This operation has O(1) complexity.
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// # use lsm_tree::{Config, Tree};
    /// #
    /// let tree = Config::new(folder).open()?;
    /// assert!(tree.is_empty()?);
    ///
    /// tree.insert("a", nanoid::nanoid!())?;
    /// assert!(!tree.is_empty()?);
    /// #
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn is_empty(&self) -> crate::Result<bool> {
        self.first_key_value().map(|x| x.is_none())
    }

    /// Creates a new tree in a folder.
    ///
    /// # Errors
    ///
    /// - Will return `Err` if an IO error occurs
    /// - Will fail, if the folder already occupied
    fn create_new(config: Config) -> crate::Result<Self> {
        use std::sync::atomic::{AtomicU32, AtomicU64};

        log::info!("Creating LSM-tree at {}", config.path.display());

        // Setup folders
        std::fs::create_dir_all(&config.path)?;
        std::fs::create_dir_all(config.path.join(PARTITIONS_FOLDER))?;
        std::fs::create_dir_all(config.path.join(JOURNALS_FOLDER))?;

        let marker = config.path.join(LSM_MARKER);
        assert!(!marker.try_exists()?);

        let first_journal_path = config
            .path
            .join(JOURNALS_FOLDER)
            .join(&*generate_segment_id());

        let block_cache = Arc::clone(&config.block_cache);

        let compaction_threads = 4; // TODO: config
        let flush_threads = config.flush_threads.into();

        let journal = Arc::new(Journal::create_new(first_journal_path)?);
        let lsn = Arc::new(AtomicU64::new(0));

        let default_partition = Partition::create_new(
            get_default_partition_key(),
            journal.clone(),
            lsn.clone(),
            config.clone(),
        )?;

        let inner = TreeInner {
            config,
            journal,
            partitions: {
                let mut map = HashMap::default();
                map.insert(get_default_partition_key(), default_partition);
                Arc::new(RwLock::new(map))
            },
            block_cache,
            next_lsn: lsn,
            flush_semaphore: Arc::new(Semaphore::new(flush_threads)),
            compaction_semaphore: Arc::new(Semaphore::new(compaction_threads)), // TODO: config
            open_snapshots: Arc::new(AtomicU32::new(0)),
            stop_signal: crate::stop_signal::StopSignal::default(),
        };

        #[cfg(not(target_os = "windows"))]
        {
            // fsync folders on Unix

            let folder = std::fs::File::open(inner.config.path.join(PARTITIONS_FOLDER))?;
            folder.sync_all()?;

            let folder = std::fs::File::open(inner.config.path.join(JOURNALS_FOLDER))?;
            folder.sync_all()?;

            let folder = std::fs::File::open(&inner.config.path)?;
            folder.sync_all()?;
        }

        // NOTE: Lastly, fsync .lsm marker, which contains the version
        // -> the LSM is fully initialized
        let mut file = std::fs::File::create(marker)?;
        Version::V0.write_file_header(&mut file)?;
        file.sync_all()?;

        Ok(Self(Arc::new(inner)))
    }

    /// Tries to recover a tree from a folder.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    fn recover(config: Config) -> crate::Result<Self> {
        crate::recovery::recover_tree(config)
    }

    /// Inserts a key-value pair into the tree.
    ///
    /// If the key already exists, the item will be overwritten.
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// # use lsm_tree::{Config, Tree};
    /// #
    /// # let tree = Config::new(folder).open()?;
    /// tree.insert("a", nanoid::nanoid!())?;
    ///
    /// assert!(!tree.is_empty()?);
    /// #
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn insert<K: AsRef<[u8]>, V: AsRef<[u8]>>(&self, key: K, value: V) -> crate::Result<()> {
        self.get_default_partition().insert(key, value)
    }

    /// Deletes an item from the tree.
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// # use lsm_tree::{Config, Tree};
    /// #
    /// # let tree = Config::new(folder).open()?;
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
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn remove<K: AsRef<[u8]>>(&self, key: K) -> crate::Result<()> {
        self.get_default_partition().remove(key)
    }

    #[doc(hidden)]
    #[must_use]
    pub fn get_default_partition(&self) -> Partition {
        self.partitions
            .read()
            .expect("lock is poisoned")
            .get(&get_default_partition_key())
            .expect("default partition should exist")
            .clone()
    }

    /// Removes the item and returns its value if it was previously in the tree.
    ///
    /// This is less efficient than just deleting because it needs to do a read before deleting.
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// # use lsm_tree::{Config, Tree};
    /// #
    /// let tree = Config::new(folder).open()?;
    ///
    /// let item = tree.remove_entry("a")?;
    /// assert_eq!(None, item);
    ///
    /// tree.insert("a", "abc")?;
    ///
    /// let item = tree.remove_entry("a")?.expect("should have removed item");
    /// assert_eq!("abc".as_bytes(), &*item);
    /// #
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn remove_entry<K: AsRef<[u8]>>(&self, key: K) -> crate::Result<Option<UserData>> {
        self.get_default_partition().remove_entry(key)
    }

    /// Returns `true` if the tree contains the specified key.
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// # use lsm_tree::{Config, Tree};
    /// #
    /// let tree = Config::new(folder).open()?;
    /// assert!(!tree.contains_key("a")?);
    ///
    /// tree.insert("a", nanoid::nanoid!())?;
    /// assert!(tree.contains_key("a")?);
    /// #
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn contains_key<K: AsRef<[u8]>>(&self, key: K) -> crate::Result<bool> {
        self.get(key).map(|x| x.is_some())
    }

    /// Returns an iterator that scans through the entire tree.
    ///
    /// Avoid using this function, or limit it as otherwise it may scan a lot of items.
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// # use lsm_tree::{Config, Tree};
    /// #
    /// # let tree = Config::new(folder).open()?;
    /// #
    /// tree.insert("a", nanoid::nanoid!())?;
    /// tree.insert("f", nanoid::nanoid!())?;
    /// tree.insert("g", nanoid::nanoid!())?;
    /// assert_eq!(3, tree.iter().into_iter().count());
    /// #
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    #[allow(clippy::iter_not_returning_iterator)]
    #[must_use]
    pub fn iter(&self) -> Range {
        self.get_default_partition().iter()
    }

    /// Returns an iterator over a range of items.
    ///
    /// Avoid using full or unbounded ranges as they may scan a lot of items (unless limited).
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// # use lsm_tree::{Config, Tree};
    /// #
    /// # let tree = Config::new(folder).open()?;
    /// #
    /// tree.insert("a", nanoid::nanoid!())?;
    /// tree.insert("f", nanoid::nanoid!())?;
    /// tree.insert("g", nanoid::nanoid!())?;
    /// assert_eq!(2, tree.range("a"..="f").into_iter().count());
    /// #
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    pub fn range<K: AsRef<[u8]>, R: RangeBounds<K>>(&self, range: R) -> Range {
        self.get_default_partition().range(range)
    }

    /// Returns an iterator over a prefixed set of items.
    ///
    /// Avoid using an empty prefix as it may scan a lot of items (unless limited).
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// # use lsm_tree::{Config, Tree};
    /// #
    /// # let tree = Config::new(folder).open()?;
    /// #
    /// tree.insert("a", nanoid::nanoid!())?;
    /// tree.insert("ab", nanoid::nanoid!())?;
    /// tree.insert("abc", nanoid::nanoid!())?;
    /// assert_eq!(2, tree.prefix("ab").into_iter().count());
    /// #
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    pub fn prefix<K: AsRef<[u8]>>(&self, prefix: K) -> Prefix {
        self.get_default_partition().prefix(prefix)
    }

    /// Returns the first key-value pair in the tree.
    ///
    /// The key in this pair is the minimum key in the tree.
    ///
    /// # Examples
    ///
    /// ```
    /// # use lsm_tree::Error as TreeError;
    /// # use lsm_tree::{Tree, Config};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let tree = Config::new(folder).open()?;
    /// #
    /// tree.insert("1", "abc")?;
    /// tree.insert("3", "abc")?;
    /// tree.insert("5", "abc")?;
    ///
    /// let (key, _) = tree.first_key_value()?.expect("item should exist");
    /// assert_eq!(&*key, "1".as_bytes());
    /// #
    /// # Ok::<(), TreeError>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn first_key_value(&self) -> crate::Result<Option<(UserKey, UserData)>> {
        self.iter().into_iter().next().transpose()
    }

    /// Returns the last key-value pair in the tree.
    ///
    /// The key in this pair is the maximum key in the tree.
    ///
    /// # Examples
    ///
    /// ```
    /// # use lsm_tree::Error as TreeError;
    /// # use lsm_tree::{Tree, Config};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let tree = Config::new(folder).open()?;
    /// #
    /// tree.insert("1", "abc")?;
    /// tree.insert("3", "abc")?;
    /// tree.insert("5", "abc")?;
    ///
    /// let (key, _) = tree.last_key_value()?.expect("item should exist");
    /// assert_eq!(&*key, "5".as_bytes());
    /// #
    /// # Ok::<(), TreeError>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn last_key_value(&self) -> crate::Result<Option<(UserKey, UserData)>> {
        self.iter().into_iter().next_back().transpose()
    }

    #[doc(hidden)]
    pub fn get_internal_entry<K: AsRef<[u8]>>(
        &self,
        key: K,
        evict_tombstone: bool,
        seqno: Option<SeqNo>,
    ) -> crate::Result<Option<Value>> {
        self.get_default_partition()
            .get_internal_entry(key, evict_tombstone, seqno)
    }

    /// Retrieves an item from the tree.
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// # use lsm_tree::{Config, Tree};
    /// #
    /// # let tree = Config::new(folder).open()?;
    /// tree.insert("a", "my_value")?;
    ///
    /// let item = tree.get("a")?;
    /// assert_eq!(Some("my_value".as_bytes().into()), item);
    /// #
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> crate::Result<Option<UserData>> {
        self.get_default_partition().get(key)
    }

    /// Compare-and-swap an entry
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    ///
    /// # Panics
    ///
    /// Panics on lock poisoning
    pub fn compare_and_swap<K: AsRef<[u8]>>(
        &self,
        key: K,
        expected: Option<&UserData>,
        next: Option<&UserData>,
    ) -> crate::Result<CompareAndSwapResult> {
        self.get_default_partition()
            .compare_and_swap(key, expected, next)
    }

    /// Atomically fetches and updates an item if it exists.
    ///
    /// Returns the previous value if the item exists.
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// # use lsm_tree::{Config, Tree};
    /// #
    /// # let tree = Config::new(folder).open()?;
    /// tree.insert("key", "a")?;
    ///
    /// let prev = tree.fetch_update("key".as_bytes(), |_| Some("b"))?.expect("item should exist");
    /// assert_eq!("a".as_bytes(), &*prev);
    ///
    /// let item = tree.get("key")?.expect("item should exist");
    /// assert_eq!("b".as_bytes(), &*item);
    ///
    /// let prev = tree.fetch_update("key", |_| None::<String>)?.expect("item should exist");
    /// assert_eq!("b".as_bytes(), &*prev);
    ///
    /// assert!(tree.is_empty()?);
    /// #
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn fetch_update<K: AsRef<[u8]>, V: AsRef<[u8]>, F: Fn(Option<&UserData>) -> Option<V>>(
        &self,
        key: K,
        f: F,
    ) -> crate::Result<Option<UserData>> {
        self.get_default_partition().fetch_update(key, f)
    }

    /// Atomically fetches and updates an item if it exists.
    ///
    /// Returns the updated value if the item exists.
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// # use lsm_tree::{Config, Tree};
    /// #
    /// # let tree = Config::new(folder).open()?;
    /// tree.insert("key", "a")?;
    ///
    /// let prev = tree.update_fetch("key", |_| Some("b"))?.expect("item should exist");
    /// assert_eq!("b".as_bytes(), &*prev);
    ///
    /// let item = tree.get("key")?.expect("item should exist");
    /// assert_eq!("b".as_bytes(), &*item);
    ///
    /// let prev = tree.update_fetch("key", |_| None::<String>)?;
    /// assert_eq!(None, prev);
    ///
    /// assert!(tree.is_empty()?);
    /// #
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn update_fetch<K: AsRef<[u8]>, V: AsRef<[u8]>, F: Fn(Option<&UserData>) -> Option<V>>(
        &self,
        key: K,
        f: F,
    ) -> crate::Result<Option<UserData>> {
        self.get_default_partition().update_fetch(key, f)
    }

    /// Force-starts a memtable flush thread.
    #[doc(hidden)]
    pub fn force_memtable_flush(
        &self,
    ) -> crate::Result<std::thread::JoinHandle<crate::Result<()>>> {
        crate::flush::start(self, &self.get_default_partition())
    }

    /// Force-starts a memtable flush thread and waits until its completely done.
    #[doc(hidden)]
    pub fn wait_for_memtable_flush(&self) -> crate::Result<()> {
        let flush_thread = self.force_memtable_flush()?;
        flush_thread.join().expect("should join")
    }

    /// Performs major compaction.
    #[doc(hidden)]
    #[must_use]
    pub fn do_major_compaction(
        &self,
        target_size: u64,
    ) -> std::thread::JoinHandle<crate::Result<()>> {
        let partition = self.get_default_partition();

        let config = self.config();
        let levels = Arc::clone(&partition.levels);
        let stop_signal = self.stop_signal.clone();
        let immutable_memtables = Arc::clone(&partition.immutable_memtables);
        let open_snapshots = Arc::clone(&self.open_snapshots);
        let block_cache = Arc::clone(&self.block_cache);

        log::info!("Starting major compaction thread");

        std::thread::spawn(move || {
            log::debug!("major compaction: acquiring levels manifest write lock");
            let level_lock = levels.write().expect("lock is poisoned");
            let compactor = crate::compaction::major::Strategy::new(target_size);
            let choice = compactor.choose(&level_lock, &config);
            drop(level_lock);

            if let crate::compaction::Choice::DoCompact(payload) = choice {
                crate::compaction::worker::do_compaction(
                    &config,
                    &levels,
                    &stop_signal,
                    &immutable_memtables,
                    &open_snapshots,
                    &block_cache,
                    &payload,
                    &partition,
                )?;
            }
            Ok(())
        })
    }

    /// Flushes the journal to disk, making sure all written data
    /// is persisted and crash-safe.
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?.into_path();
    /// # use lsm_tree::{Config, Tree};
    /// #
    /// # let tree = Config::new(folder.clone()).open()?;
    /// tree.insert("a", nanoid::nanoid!())?;
    /// tree.flush()?;
    ///
    /// let tree = Config::new(folder).open()?;
    ///
    /// let item = tree.get("a")?;
    /// assert!(item.is_some());
    /// #
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn flush(&self) -> crate::Result<()> {
        self.journal.flush()?;
        Ok(())
    }

    /// Gives access to a tree data partition.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn partition<P: AsRef<str>>(&self, name: P) -> crate::Result<Partition> {
        let lock = self.partitions.read().expect("lock is poisoned");

        let partition = lock.get(name.as_ref());

        if let Some(partition) = partition {
            Ok(partition.clone())
        } else {
            drop(lock);

            let partition = Partition::create_new(
                name.as_ref().into(),
                self.journal.clone(),
                self.next_lsn.clone(),
                self.config.clone(),
            )?;

            self.partitions
                .write()
                .expect("lock is poisoned")
                .insert(name.as_ref().into(), partition.clone());

            Ok(partition)
        }
    }
}
