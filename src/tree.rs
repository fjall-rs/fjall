use crate::{
    compaction::CompactionStrategy,
    file::{JOURNALS_FOLDER, LEVELS_MANIFEST_FILE, LSM_MARKER, SEGMENTS_FOLDER},
    id::generate_segment_id,
    journal::{shard::JournalShard, Journal},
    levels::Levels,
    memtable::MemTable,
    prefix::Prefix,
    range::{MemTableGuard, Range},
    tree_inner::TreeInner,
    value::{SeqNo, UserData, UserKey, ValueType},
    version::Version,
    Batch, Config, Snapshot, Value,
};
use std::{
    ops::RangeBounds,
    sync::{Arc, RwLock, RwLockWriteGuard},
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
#[doc(alias = "keyspace")]
#[doc(alias = "table")]
#[derive(Clone)]
pub struct Tree(pub(crate) Arc<TreeInner>);

impl std::ops::Deref for Tree {
    type Target = Arc<TreeInner>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

fn ignore_tombstone_value(item: Value) -> Option<Value> {
    if item.is_tombstone() {
        None
    } else {
        Some(item)
    }
}

impl Tree {
    /// Opens the tree at the given folder.
    ///
    /// Will create a new tree if the folder is not in use
    /// or recover a previous state if it exists.
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// use lsm_tree::{Config, Tree};
    ///
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

    /// Gets the given key’s corresponding entry in the map for in-place manipulation.
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// use lsm_tree::{Config, Tree};
    ///
    /// let tree = Tree::open(Config::new(folder))?;
    ///
    /// let value = tree.entry("a")?.or_insert("abc")?;
    /// assert_eq!("abc".as_bytes(), &*value);
    /// #
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn entry<K: AsRef<[u8]>>(&self, key: K) -> crate::Result<crate::entry::Entry> {
        use crate::entry::{OccupiedEntry, VacantEntry};

        let key = key.as_ref();
        let item = self.get_internal_entry(key, true, None)?;

        Ok(match item {
            Some(item) => crate::entry::Entry::Occupied(OccupiedEntry {
                tree: self.clone(),
                key: key.to_vec().into(),
                value: item.value,
            }),
            None => crate::entry::Entry::Vacant(VacantEntry {
                tree: self.clone(),
                key: key.to_vec().into(),
            }),
        })
    }

    /// Opens a read-only point-in-time snapshot of the tree
    ///
    /// Dropping the snapshot will close the snapshot
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// use lsm_tree::{Config, Tree};
    ///
    /// let tree = Config::new(folder).open()?;
    ///
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
    /// use lsm_tree::{Config, Tree};
    ///
    /// let tree = Config::new(folder).open()?;
    ///
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
        let levels = self.levels.read().expect("lock is poisoned");
        levels.is_compacting()
    }

    /// Counts the amount of segments currently in the tree.
    #[must_use]
    pub(crate) fn first_level_segment_count(&self) -> usize {
        self.levels
            .read()
            .expect("lock is poisoned")
            .first_level_segment_count()
    }

    /// Counts the amount of segments currently in the tree.
    #[doc(hidden)]
    #[must_use]
    pub fn segment_count(&self) -> usize {
        self.levels.read().expect("lock is poisoned").len()
    }

    /// Sums the disk space usage of the tree (segments + journals).
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// use lsm_tree::{Config, Tree};
    ///
    /// let tree = Config::new(folder).open()?;
    /// assert_eq!(0, tree.disk_space()?);
    /// #
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    pub fn disk_space(&self) -> crate::Result<u64> {
        let segment_size = self
            .levels
            .read()
            .expect("lock is poisoned")
            .get_all_segments()
            .values()
            .map(|x| x.metadata.file_size)
            .sum::<u64>();

        let _lock = self.journal.shards.full_lock();

        // TODO: replace fs extra with Journal::disk_space
        let active_journal_size = fs_extra::dir::get_size(&self.journal.path)
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "fs_extra error"))?;

        Ok(segment_size + active_journal_size)
    }

    /// Approximates the item count of the tree.
    ///
    /// This metric is only reliable for insert-only (no updates, deletes) workloads.
    /// Otherwise, the value may become less accurate over time
    /// and only converge to the real value time as compaction kicks in.
    ///
    /// This operation has O(1) complexity and can be used
    /// without feeling bad about it.
    pub fn approximate_len(&self) -> crate::Result<u64> {
        let segment_size = self
            .levels
            .read()
            .expect("lock is poisoned")
            .get_all_segments()
            .values()
            .map(|x| x.metadata.key_count)
            .sum::<u64>();

        let active_memtable_size = self
            .active_memtable
            .read()
            .expect("lock is poisoned")
            .items
            .len() as u64;

        let immutable_memtables_sizes = self
            .immutable_memtables
            .read()
            .iter()
            .map(|x| x.len() as u64)
            .sum::<u64>();

        Ok(segment_size + active_memtable_size + immutable_memtables_sizes)
    }

    /// Returns the tree configuration.
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// use lsm_tree::{Config, Tree};
    ///
    /// let tree = Config::new(folder).open()?;
    ///
    /// assert_eq!(Config::default().block_size, tree.config().block_size);
    /// #
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    #[must_use]
    pub fn config(&self) -> Config {
        self.config.clone()
    }

    /// Returns the amount of cached blocks.
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// use lsm_tree::{Config, Tree};
    ///
    /// let tree = Config::new(folder).open()?;
    ///
    /// assert_eq!(0, tree.block_cache_size());
    /// #
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    #[must_use]
    pub fn block_cache_size(&self) -> usize {
        self.block_cache.len()
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
    /// use lsm_tree::{Tree, Config};
    ///
    /// let folder = tempfile::tempdir()?;
    /// let tree = Config::new(folder).open()?;
    ///
    /// assert_eq!(tree.len()?, 0);
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
        Ok(self.iter().into_iter().filter(Result::is_ok).count())
    }

    /// Returns `true` if the tree is empty.
    ///
    /// This operation has O(1) complexity.
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// use lsm_tree::{Config, Tree};
    ///
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
        std::fs::create_dir_all(config.path.join(SEGMENTS_FOLDER))?;
        std::fs::create_dir_all(config.path.join(JOURNALS_FOLDER))?;

        let marker = config.path.join(LSM_MARKER);
        assert!(!marker.try_exists()?);

        let first_journal_path = config
            .path
            .join(JOURNALS_FOLDER)
            .join(&*generate_segment_id());

        let levels =
            Levels::create_new(config.level_count, config.path.join(LEVELS_MANIFEST_FILE))?;

        let block_cache = Arc::clone(&config.block_cache);

        let compaction_threads = 4; // TODO: config
        let flush_threads = config.flush_threads.into();

        let inner = TreeInner {
            config,
            journal: Arc::new(Journal::create_new(first_journal_path)?),
            active_memtable: Arc::new(RwLock::new(MemTable::default())),
            immutable_memtables: Arc::default(),
            block_cache,
            next_lsn: AtomicU64::new(0),
            levels: Arc::new(RwLock::new(levels)),
            flush_semaphore: Arc::new(Semaphore::new(flush_threads)),
            compaction_semaphore: Arc::new(Semaphore::new(compaction_threads)), // TODO: config
            approx_active_memtable_size: AtomicU32::default(),
            open_snapshots: Arc::new(AtomicU32::new(0)),
            stop_signal: crate::stop_signal::StopSignal::default(),
        };

        #[cfg(not(target_os = "windows"))]
        {
            // fsync folders on Unix
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

    fn append_entry(
        &self,
        mut shard: RwLockWriteGuard<'_, JournalShard>,
        value: Value,
    ) -> crate::Result<()> {
        let bytes_written_to_disk = shard.write(&value)?;
        drop(shard);

        let memtable_lock = self.active_memtable.read().expect("lock is poisoned");
        memtable_lock.insert(value);

        // NOTE: Add some pointers to better approximate memory usage of memtable
        // Because the data is stored with less overhead than in memory
        let size = bytes_written_to_disk
            + std::mem::size_of::<UserKey>()
            + std::mem::size_of::<UserData>();

        let memtable_size = self
            .approx_active_memtable_size
            .fetch_add(size as u32, std::sync::atomic::Ordering::Relaxed);

        drop(memtable_lock);

        if memtable_size > self.config.max_memtable_size {
            log::debug!("Memtable reached threshold size");

            while self.first_level_segment_count() > 32 {
                // NOTE: Spin lock to stall writes
                // It's not beautiful, but better than
                // running out of file descriptors and crashing
                //
                // TODO: maybe make this configurable

                log::warn!("Write stall!");
                std::thread::sleep(std::time::Duration::from_millis(100));
            }

            log::debug!("Flushing active memtable");
            crate::flush::start(self)?;
        }

        Ok(())
    }

    /// Inserts a key-value pair into the tree.
    ///
    /// If the key already exists, the item will be overwritten.
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// use lsm_tree::{Config, Tree};
    ///
    /// let tree = Config::new(folder).open()?;
    /// tree.insert("a", nanoid::nanoid!())?;
    /// #
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn insert<K: AsRef<[u8]>, V: AsRef<[u8]>>(&self, key: K, value: V) -> crate::Result<()> {
        let shard = self.journal.lock_shard();

        let value = Value::new(
            key.as_ref(),
            value.as_ref(),
            self.next_lsn
                .fetch_add(1, std::sync::atomic::Ordering::AcqRel),
            ValueType::Value,
        );

        self.append_entry(shard, value)?;

        Ok(())
    }

    /// Deletes an item from the tree.
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// use lsm_tree::{Config, Tree};
    ///
    /// let tree = Config::new(folder).open()?;
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
        let shard = self.journal.lock_shard();

        let value = Value::new(
            key.as_ref(),
            vec![],
            self.next_lsn
                .fetch_add(1, std::sync::atomic::Ordering::AcqRel),
            ValueType::Tombstone,
        );

        self.append_entry(shard, value)?;

        Ok(())
    }

    /// Removes the item and returns its value if it was previously in the tree.
    ///
    /// This is less efficient than just deleting because it needs to do a read before deleting.
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// use lsm_tree::{Config, Tree};
    ///
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
        self.fetch_update(key, |_| None::<UserData>)
    }

    /// Returns `true` if the tree contains the specified key.
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// use lsm_tree::{Config, Tree};
    ///
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
    pub fn contains_key<K: AsRef<[u8]> + std::hash::Hash>(&self, key: K) -> crate::Result<bool> {
        self.get(key).map(|x| x.is_some())
    }

    pub(crate) fn create_iter(&self, seqno: Option<SeqNo>) -> Range<'_> {
        self.create_range::<UserKey, _>(.., seqno)
    }

    #[allow(clippy::iter_not_returning_iterator)]
    /// Returns an iterator that scans through the entire tree.
    ///
    /// Avoid using this function, or limit it as otherwise it may scan a lot of items.
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// use lsm_tree::{Config, Tree};
    ///
    /// let tree = Config::new(folder).open()?;
    ///
    /// tree.insert("a", nanoid::nanoid!())?;
    /// tree.insert("f", nanoid::nanoid!())?;
    /// tree.insert("g", nanoid::nanoid!())?;
    /// assert_eq!(3, tree.iter().into_iter().count());
    /// #
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    #[must_use]
    pub fn iter(&self) -> Range<'_> {
        self.create_iter(None)
    }

    pub(crate) fn create_range<K: AsRef<[u8]>, R: RangeBounds<K>>(
        &self,
        range: R,
        seqno: Option<SeqNo>,
    ) -> Range<'_> {
        use std::ops::Bound::{self, Excluded, Included, Unbounded};

        let lo: Bound<UserKey> = match range.start_bound() {
            Included(x) => Included(x.as_ref().into()),
            Excluded(x) => Excluded(x.as_ref().into()),
            Unbounded => Unbounded,
        };

        let hi: Bound<UserKey> = match range.end_bound() {
            Included(x) => Included(x.as_ref().into()),
            Excluded(x) => Excluded(x.as_ref().into()),
            Unbounded => Unbounded,
        };

        let bounds: (Bound<UserKey>, Bound<UserKey>) = (lo, hi);

        let lock = self.levels.read().expect("lock is poisoned");

        let segment_info = lock
            .get_all_segments()
            .values()
            .filter(|x| x.check_key_range_overlap(&bounds))
            .cloned()
            .collect::<Vec<_>>();

        Range::new(
            crate::range::MemTableGuard {
                active: self.active_memtable.read().expect("lock is poisoned"),
                immutable: self.immutable_memtables.read().expect("lock is poisoned"),
            },
            bounds,
            segment_info,
            seqno,
        )
    }

    /// Returns an iterator over a range of items.
    ///
    /// Avoid using full or unbounded ranges as they may scan a lot of items (unless limited).
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// use lsm_tree::{Config, Tree};
    ///
    /// let tree = Config::new(folder).open()?;
    ///
    /// tree.insert("a", nanoid::nanoid!())?;
    /// tree.insert("f", nanoid::nanoid!())?;
    /// tree.insert("g", nanoid::nanoid!())?;
    /// assert_eq!(2, tree.range("a"..="f").into_iter().count());
    /// #
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn range<K: AsRef<[u8]>, R: RangeBounds<K>>(&self, range: R) -> Range<'_> {
        self.create_range(range, None)
    }

    pub(crate) fn create_prefix<K: Into<UserKey>>(
        &self,
        prefix: K,
        seqno: Option<SeqNo>,
    ) -> Prefix<'_> {
        let prefix = prefix.into();

        let lock = self.levels.read().expect("lock is poisoned");

        let segment_info = lock
            .get_all_segments()
            .values()
            .filter(|x| x.check_prefix_overlap(&prefix))
            .cloned()
            .collect();

        Prefix::new(
            MemTableGuard {
                active: self.active_memtable.read().expect("lock is poisoned"),
                immutable: self.immutable_memtables.read().expect("lock is poisoned"),
            },
            prefix,
            segment_info,
            seqno,
        )
    }

    /// Returns an iterator over a prefixed set of items.
    ///
    /// Avoid using an empty prefix as it may scan a lot of items (unless limited).
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// use lsm_tree::{Config, Tree};
    ///
    /// let tree = Config::new(folder).open()?;
    ///
    /// tree.insert("a", nanoid::nanoid!())?;
    /// tree.insert("ab", nanoid::nanoid!())?;
    /// tree.insert("abc", nanoid::nanoid!())?;
    /// assert_eq!(2, tree.prefix("ab").into_iter().count());
    /// #
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn prefix<K: AsRef<[u8]>>(&self, prefix: K) -> Prefix<'_> {
        self.create_prefix(prefix.as_ref(), None)
    }

    /// Returns the first key-value pair in the tree.
    /// The key in this pair is the minimum key in the tree.
    ///
    /// # Examples
    ///
    /// ```
    /// # use lsm_tree::Error as TreeError;
    /// use lsm_tree::{Tree, Config};
    ///
    /// # let folder = tempfile::tempdir()?;
    /// let tree = Config::new(folder).open()?;
    ///
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
    /// The key in this pair is the maximum key in the tree.
    ///
    /// # Examples
    ///
    /// ```
    /// # use lsm_tree::Error as TreeError;
    /// use lsm_tree::{Tree, Config};
    ///
    /// # let folder = tempfile::tempdir()?;
    /// let tree = Config::new(folder).open()?;
    ///
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
    pub fn get_internal_entry<K: AsRef<[u8]> + std::hash::Hash>(
        &self,
        key: K,
        evict_tombstone: bool,
        seqno: Option<SeqNo>,
    ) -> crate::Result<Option<Value>> {
        let memtable_lock = self.active_memtable.read().expect("lock is poisoned");

        if let Some(item) = memtable_lock.get(&key, seqno) {
            if evict_tombstone {
                return Ok(ignore_tombstone_value(item));
            }
            return Ok(Some(item));
        };
        drop(memtable_lock);

        // Now look in immutable memtables
        let memtable_lock = self.immutable_memtables.read().expect("lock is poisoned");
        for (_, memtable) in memtable_lock.iter().rev() {
            if let Some(item) = memtable.get(&key, seqno) {
                if evict_tombstone {
                    return Ok(ignore_tombstone_value(item));
                }
                return Ok(Some(item));
            }
        }
        drop(memtable_lock);

        // Now look in segments... this may involve disk I/O
        let segment_lock = self.levels.read().expect("lock is poisoned");
        let segments = &segment_lock.get_all_segments_flattened();

        for segment in segments {
            if let Some(item) = segment.get(&key, seqno)? {
                if evict_tombstone {
                    return Ok(ignore_tombstone_value(item));
                }
                return Ok(Some(item));
            }
        }

        Ok(None)
    }

    /// Retrieves an item from the tree.
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// use lsm_tree::{Config, Tree};
    ///
    /// let tree = Config::new(folder).open()?;
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
    pub fn get<K: AsRef<[u8]> + std::hash::Hash>(&self, key: K) -> crate::Result<Option<UserData>> {
        Ok(self.get_internal_entry(key, true, None)?.map(|x| x.value))
    }

    pub(crate) fn increment_lsn(&self) -> SeqNo {
        self.next_lsn
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel)
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
        let key = key.as_ref();

        // NOTE: Not sure if this is the implementation
        // but going with correctness over performance here
        // The rationale behind locking everything is:
        //
        // Imagine there was no locking:
        // (1) We start a CAS, and read a key to compare it
        // (2) Our thread pauses
        // (3) Another thread updates the item, it is now different
        // (4) Our thread proceeds: it now compares to an older value (the other item is never considered)
        // (5) The CAS is inconsistent
        //
        // With locking:
        // (1) We start a CAS, and read a key to compare it
        // (2) Our thread pauses
        // (3) Another thread wants to update the item, but cannot find an open shard
        // (4) Our thread proceeds: it now does the CAS, and unlocks all shards
        // (5) The other thread now takes a shard, and gets the most up-to-date value
        //     (the one we just CAS'ed)
        let mut journal_lock = self.journal.shards.full_lock().expect("lock is poisoned");
        let shard = journal_lock.pop().expect("journal should have shards");

        match self.get(key)? {
            Some(current_value) => {
                match expected {
                    Some(expected_value) => {
                        // We expected Some and got Some
                        // Check if the value is as expected
                        if current_value != *expected_value {
                            return Ok(Err(CompareAndSwapError {
                                prev: Some(current_value),
                                next: next.cloned(),
                            }));
                        }

                        // Set or delete the object now
                        if let Some(next_value) = next {
                            self.append_entry(
                                shard,
                                Value {
                                    key: key.into(),
                                    value: next_value.clone(),
                                    seqno: self.increment_lsn(),
                                    value_type: ValueType::Value,
                                },
                            )?;
                        } else {
                            self.append_entry(
                                shard,
                                Value {
                                    key: key.into(),
                                    value: [].into(),
                                    seqno: self.increment_lsn(),
                                    value_type: ValueType::Tombstone,
                                },
                            )?;
                        }

                        Ok(Ok(()))
                    }
                    None => {
                        // We expected Some but got None
                        // CAS error!
                        Ok(Err(CompareAndSwapError {
                            prev: None,
                            next: next.cloned(),
                        }))
                    }
                }
            }
            None => match expected {
                Some(_) => {
                    // We expected Some but got None
                    // CAS error!
                    Ok(Err(CompareAndSwapError {
                        prev: None,
                        next: next.cloned(),
                    }))
                }
                None => match next {
                    // We expected None and got None

                    // Set the object now
                    Some(next_value) => {
                        self.append_entry(
                            shard,
                            Value {
                                key: key.into(),
                                value: next_value.clone(),
                                seqno: self.increment_lsn(),
                                value_type: ValueType::Value,
                            },
                        )?;
                        Ok(Ok(()))
                    }
                    // Item is already deleted, do nothing
                    None => Ok(Ok(())),
                },
            },
        }
    }

    /// Atomically fetches and updates an item if it exists.
    ///
    /// Returns the previous value if the item exists.
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// use lsm_tree::{Config, Tree};
    ///
    /// let tree = Config::new(folder).open()?;
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
        let key = key.as_ref();

        let mut fetched = self.get(key)?;

        loop {
            let expected = fetched.as_ref();
            let next = f(expected).map(|v| v.as_ref().into());

            match self.compare_and_swap(key, expected, next.as_ref())? {
                Ok(()) => return Ok(fetched),
                Err(err) => {
                    fetched = err.prev;
                }
            }
        }
    }

    /// Atomically fetches and updates an item if it exists.
    ///
    /// Returns the updated value if the item exists.
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// use lsm_tree::{Config, Tree};
    ///
    /// let tree = Config::new(folder).open()?;
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
        let key = key.as_ref();

        let mut fetched = self.get(key)?;

        loop {
            let expected = fetched.as_ref();
            let next = f(expected).map(|v| v.as_ref().into());

            match self.compare_and_swap(key, expected, next.as_ref())? {
                Ok(()) => return Ok(next),
                Err(err) => {
                    fetched = err.prev;
                }
            }
        }
    }

    /// Force-starts a memtable flush thread.
    #[doc(hidden)]
    pub fn force_memtable_flush(
        &self,
    ) -> crate::Result<std::thread::JoinHandle<crate::Result<()>>> {
        crate::flush::start(self)
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
        log::info!("Starting major compaction thread");

        let config = self.config();
        let levels = Arc::clone(&self.levels);
        let stop_signal = self.stop_signal.clone();
        let immutable_memtables = Arc::clone(&self.immutable_memtables);
        let open_snapshots = Arc::clone(&self.open_snapshots);
        let block_cache = Arc::clone(&self.block_cache);

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
    /// use lsm_tree::{Config, Tree};
    ///
    /// let tree = Config::new(folder.clone()).open()?;
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
}
