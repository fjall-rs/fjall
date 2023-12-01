use crate::{
    block_cache::BlockCache,
    commit_log::CommitLog,
    compaction::{worker::start_compaction_thread, CompactionStrategy},
    id::generate_segment_id,
    levels::Levels,
    memtable::MemTable,
    prefix::Prefix,
    range::{MemTableGuard, Range},
    segment::{self, meta::Metadata, Segment},
    tree_inner::TreeInner,
    Batch, Config, Value,
};
use std::{
    collections::HashMap,
    ops::RangeBounds,
    path::{Path, PathBuf},
    sync::{atomic::AtomicU64, Arc, Mutex, MutexGuard, RwLock},
};
use std_semaphore::Semaphore;

/// A log-structured merge tree (LSM-tree/LSMT)
///
/// The tree is internally synchronized (Send + Sync), so it does not need to be wrapped in a lock nor an Arc.
///
/// To share the tree with threads, use `Arc::clone(&tree)` or `tree.clone()`.
#[doc(alias = "keyspace")]
#[doc(alias = "table")]
#[derive(Clone)]
pub struct Tree(Arc<TreeInner>);

impl std::ops::Deref for Tree {
    type Target = Arc<TreeInner>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

fn ignore_tombstone_value(item: Value) -> Option<Value> {
    if item.is_tombstone {
        None
    } else {
        Some(item)
    }
}

impl Tree {
    /// Opens the tree at the given folder.
    ///
    /// Will create a new tree if the folder is not in use or recover a previous state
    /// if it exists
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
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// - Will return `Err` if an IO error occurs
    pub fn open(config: Config) -> crate::Result<Self> {
        if config.path.join(".lsm").exists() {
            Self::recover(config)
        } else {
            Self::create_new(config)
        }
    }

    /// Initializes a new, atomic write batch
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
    ///
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    ///
    /// Call [`Batch::commit`] to commit the batch to the LSM-tree
    #[must_use]
    pub fn batch(&self) -> Batch {
        Batch::new(self.clone())
    }

    /// Returns `true` if there are some segments that are being compacted
    #[doc(hidden)]
    #[must_use]
    pub fn is_compacting(&self) -> bool {
        let levels = self.levels.read().expect("lock is poisoned");
        levels.is_compacting()
    }

    /// Counts the amount of segments currently in the tree
    #[must_use]
    pub fn segment_count(&self) -> usize {
        self.levels.read().expect("lock is poisoned").len()
    }

    /// Sums the disk space usage of the tree (segments + commit log)
    #[must_use]
    pub fn disk_space(&self) -> u64 {
        let segment_size: u64 = self
            .levels
            .read()
            .expect("lock is poisoned")
            .get_all_segments()
            .values()
            .map(|x| x.metadata.file_size)
            .sum();

        let memtable = self.active_memtable.read().expect("lock is poisoned");

        segment_size + u64::from(memtable.size_in_bytes)
    }

    /// Returns the folder path used by the tree
    #[must_use]
    pub fn path(&self) -> PathBuf {
        self.config.path.clone()
    }

    /// Scans the entire Tree, returning the amount of items
    ///
    /// # Example usage
    ///
    /// ```
    /// # use lsm_tree::Error as TreeError;
    /// # use lsm_tree::{Tree, Config};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let mut tree = Config::new(folder).open()?;
    /// #
    /// assert_eq!(tree.len()?, 0);
    /// tree.insert("1", "abc")?;
    /// tree.insert("3", "abc")?;
    /// tree.insert("5", "abc")?;
    /// assert_eq!(tree.len()?, 3);
    ///
    /// # Ok::<(), TreeError>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs
    #[deprecated(
        note = "len() isn't deprecated per se, however it performs a full tree scan and should be avoided"
    )]
    pub fn len(&self) -> crate::Result<usize> {
        Ok(self.iter()?.into_iter().filter(Result::is_ok).count())
    }

    /// Returns `true` if the tree is empty
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
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// - Will return `Err` if an IO error occurs
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
        std::fs::create_dir_all(&config.path)?;

        let marker = config.path.join(".lsm");
        assert!(!marker.try_exists()?);

        // fsync .lsm marker
        let file = std::fs::File::create(marker)?;
        file.sync_all()?;

        let log_path = config.path.join("log");
        let levels = Levels::create_new(config.levels, config.path.join("levels.json"))?;

        let block_cache = Arc::new(BlockCache::new(config.block_cache_size as usize));

        let flush_threads = config.flush_threads.into();

        let inner = TreeInner {
            config,
            active_memtable: Arc::new(RwLock::new(MemTable::default())),
            immutable_memtables: Arc::default(),
            commit_log: Arc::new(Mutex::new(CommitLog::new(log_path)?)),
            block_cache,
            lsn: AtomicU64::new(0),
            levels: Arc::new(RwLock::new(levels)),
            flush_semaphore: Arc::new(Semaphore::new(flush_threads)),
            compaction_semaphore: Arc::new(Semaphore::new(4)), // TODO: config
        };

        // Create subfolders
        std::fs::create_dir_all(inner.config.path.join("segments"))?;
        std::fs::create_dir_all(inner.config.path.join("logs"))?;

        // fsync folder
        let folder = std::fs::File::open(&inner.config.path)?;
        folder.sync_all()?;

        Ok(Self(Arc::new(inner)))
    }

    fn recover_segments<P: AsRef<Path>>(
        folder: &P,
        block_cache: &Arc<BlockCache>,
    ) -> crate::Result<HashMap<String, Arc<Segment>>> {
        let folder = folder.as_ref();

        // NOTE: First we load the level manifest without any
        // segments just to get the IDs
        // Then we recover the segments and build the actual level manifest
        let levels = Levels::recover(&folder.join("levels.json"), HashMap::new())?;
        let segment_ids_to_recover = levels.list_ids();

        let mut segments = HashMap::new();

        for dirent in std::fs::read_dir(folder.join("segments"))? {
            let dirent = dirent?;
            let path = dirent.path();

            assert!(path.is_dir());

            let segment_id = dirent.file_name().to_str().unwrap().to_owned();
            log::debug!("Recovering segment from {}", path.display());

            if segment_ids_to_recover.contains(&segment_id) {
                let segment = Segment::recover(&path, Arc::clone(block_cache))?;
                segments.insert(segment.metadata.id.clone(), Arc::new(segment));
                log::debug!("Recovered segment from {}", path.display());
            } else {
                log::info!("Deleting unfinished segment: {}", path.to_string_lossy());
                std::fs::remove_dir_all(path)?;
            }
        }

        if segments.len() < segment_ids_to_recover.len() {
            log::error!("Expected segments : {segment_ids_to_recover:?}");
            log::error!(
                "Recovered segments: {:?}",
                segments.keys().collect::<Vec<_>>()
            );

            panic!("Some segments were not recovered")
        }

        Ok(segments)
    }

    /// Tries to recover a tree from a folder.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs
    fn recover(config: Config) -> crate::Result<Self> {
        let start = std::time::Instant::now();

        // Flush orphaned logs

        // NOTE: Load previous levels manifest
        // Add all flushed segments to it, then recover properly
        let mut levels = Levels::recover(&config.path.join("levels.json"), HashMap::new())?;

        for dirent in std::fs::read_dir(&config.path.join("logs"))? {
            let dirent = dirent?;

            log::info!(
                "Flushing orphaned journal {} to segment",
                dirent.path().to_string_lossy()
            );

            let (_, _, memtable) = MemTable::from_file(dirent.path()).unwrap();

            let segment_id = generate_segment_id();
            let segment_folder = config.path.join("segments").join(&segment_id);

            let mut segment_writer = segment::writer::Writer::new(segment::writer::Options {
                path: segment_folder.clone(),
                evict_tombstones: false,
                block_size: config.block_size,
            })?;

            for (key, value) in memtable.items {
                segment_writer.write(Value::from((key, value)))?;
            }

            segment_writer.finish()?;

            let metadata = Metadata::from_writer(segment_id, segment_writer);
            metadata.write_to_file()?;

            log::info!("Written segment from orphaned journal: {:?}", metadata.id);

            levels.add_id(metadata.id);
            levels.write_to_disk()?;

            // TODO: if an IO happens here, that'd be bad
            // TODO: because on NEXT restart it would be flushed again
            // TODO: the log file/(folder when sharded) should have the same ID as the segment
            // TODO: so the log can be discarded
            std::fs::remove_file(dirent.path())?;
        }

        // Restore memtable from current commit log

        log::info!("Restoring memtable");

        let (mut lsn, _, memtable) = MemTable::from_file(config.path.join("log")).unwrap();

        // Load segments

        log::info!("Restoring segments");

        let block_cache = Arc::new(BlockCache::new(config.block_cache_size as usize));
        let segments = Self::recover_segments(&config.path, &block_cache)?;

        // Check if a segment has a higher seqno and then take it
        lsn = lsn.max(
            segments
                .values()
                .map(|x| x.metadata.seqnos.1)
                .max()
                .unwrap_or(0),
        );

        // Finalize Tree

        log::debug!("Loading level manifest");

        let mut levels = Levels::recover(&config.path.join("levels.json"), segments)?;
        levels.sort_levels();

        let log_path = config.path.join("log");

        let compaction_threads = 4; // TODO: config
        let flush_threads = config.flush_threads.into();

        let inner = TreeInner {
            config,
            active_memtable: Arc::new(RwLock::new(memtable)),
            immutable_memtables: Arc::default(),
            block_cache,
            commit_log: Arc::new(Mutex::new(CommitLog::new(log_path)?)),
            lsn: AtomicU64::new(lsn),
            levels: Arc::new(RwLock::new(levels)),
            flush_semaphore: Arc::new(Semaphore::new(flush_threads)),
            compaction_semaphore: Arc::new(Semaphore::new(compaction_threads)),
        };

        let tree = Self(Arc::new(inner));

        log::debug!("Starting {compaction_threads} compaction threads");
        for _ in 0..compaction_threads {
            start_compaction_thread(&tree);
        }

        log::info!("Tree loaded in {}s", start.elapsed().as_secs_f32());

        Ok(tree)
    }

    fn append_entry(
        &self,
        mut commit_log: MutexGuard<CommitLog>,
        value: Value,
    ) -> crate::Result<()> {
        let mut memtable = self.active_memtable.write().expect("lock is poisoned");

        commit_log.append(value.clone())?;

        // NOTE: Add value key length to take into account the overhead of keys
        // inside the MemTable
        let size = value.size() + value.key.len();
        memtable.insert(value);
        memtable.size_in_bytes += size as u32;

        if memtable.exceeds_threshold(self.config.max_memtable_size) {
            crate::flush::start(self, commit_log, memtable)?;
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
    ///
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs
    pub fn insert<K: Into<Vec<u8>>, V: Into<Vec<u8>>>(
        &self,
        key: K,
        value: V,
    ) -> crate::Result<()> {
        let commit_log = self.commit_log.lock().expect("lock is poisoned");

        let value = Value::new(
            key,
            value,
            false,
            self.lsn.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
        );

        self.append_entry(commit_log, value)?;

        Ok(())
    }

    /// Deletes an item from the tree
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// use lsm_tree::{Config, Tree};
    ///
    /// let tree = Config::new(folder).open()?;
    /// tree.insert("a", nanoid::nanoid!())?;
    ///
    /// let item = tree.get("a")?;
    /// assert!(item.is_some());
    ///
    /// tree.remove("a")?;
    ///
    /// let item = tree.get("a")?;
    /// assert!(item.is_none());
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs
    pub fn remove<K: Into<Vec<u8>>>(&self, key: K) -> crate::Result<()> {
        let commit_log = self.commit_log.lock().expect("lock is poisoned");

        let value = Value::new(
            key,
            vec![],
            true,
            self.lsn.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
        );

        self.append_entry(commit_log, value)?;

        Ok(())
    }

    /// Returns `true` if the tree contains the specified key
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
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs
    pub fn contains_key<K: AsRef<[u8]>>(&self, key: K) -> crate::Result<bool> {
        self.get(key).map(|x| x.is_some())
    }

    /// Returns `true` if the tree contains the specified item
    ///
    /// Alias for [`Tree::contains_key`]
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// use lsm_tree::{Config, Tree};
    ///
    /// let tree = Config::new(folder).open()?;
    /// assert!(!tree.contains("a")?);
    ///
    /// tree.insert("a", nanoid::nanoid!())?;
    /// assert!(tree.contains("a")?);
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs
    pub fn contains<K: AsRef<[u8]>>(&self, key: K) -> crate::Result<bool> {
        self.contains_key(key)
    }

    #[allow(clippy::iter_not_returning_iterator)]
    /// Returns an iterator that scans through the entire Tree
    ///
    /// Avoid using this function, or limit it as otherwise it may scan a lot of items
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs
    pub fn iter(&self) -> crate::Result<Range<'_>> {
        self.range::<Vec<u8>, _>(..)
    }

    /// Returns an iterator over a range of items
    ///
    /// Avoid using full or unbounded ranges as they may scan a lot of items (unless limited)
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs
    pub fn range<K: AsRef<[u8]>, R: RangeBounds<K>>(&self, range: R) -> crate::Result<Range<'_>> {
        use std::ops::Bound::{self, Excluded, Included, Unbounded};

        let lo: Bound<Vec<u8>> = match range.start_bound() {
            Included(x) => Included(x.as_ref().into()),
            Excluded(x) => Excluded(x.as_ref().into()),
            Unbounded => Unbounded,
        };

        let hi: Bound<Vec<u8>> = match range.end_bound() {
            Included(x) => Included(x.as_ref().into()),
            Excluded(x) => Excluded(x.as_ref().into()),
            Unbounded => Unbounded,
        };

        let bounds: (Bound<Vec<u8>>, Bound<Vec<u8>>) = (lo, hi);

        let lock = self.levels.read().expect("lock poisoned");

        let segment_info = lock
            .get_all_segments()
            .values()
            .filter(|x| x.check_key_range_overlap(&bounds))
            .cloned()
            .collect::<Vec<_>>();

        Ok(Range::new(
            crate::range::MemTableGuard {
                active: self.active_memtable.read().expect("lock is poisoned"),
                immutable: self.immutable_memtables.read().expect("lock is poisoned"),
            },
            bounds,
            segment_info,
        ))
    }

    /// Returns an iterator over a prefixed set of items
    ///
    /// Avoid using an empty prefix as it may scan a lot of items (unless limited)
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs
    pub fn prefix<K: Into<Vec<u8>>>(&self, prefix: K) -> crate::Result<Prefix<'_>> {
        use std::ops::Bound::{self};

        let prefix = prefix.into();

        let lock = self.levels.read().expect("lock poisoned");

        let bounds: (Bound<Vec<u8>>, Bound<Vec<u8>>) =
            (Bound::Included(prefix.clone()), std::ops::Bound::Unbounded);

        let segment_info = lock
            .get_all_segments()
            .values()
            .filter(|x| x.check_key_range_overlap(&bounds))
            .cloned()
            .collect();

        Ok(Prefix::new(
            MemTableGuard {
                active: self.active_memtable.read().expect("lock poisoned"),
                immutable: self.immutable_memtables.read().expect("lock poisoned"),
            },
            prefix,
            segment_info,
        ))
    }

    /// Returns the first key-value pair in the LSM-tree. The key in this pair is the minimum key in the LSM-tree
    ///
    /// # Example usage
    ///
    /// ```
    /// # use lsm_tree::Error as TreeError;
    /// # use lsm_tree::{Tree, Config};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let mut tree = Config::new(folder).open()?;
    /// #
    /// tree.insert("1", "abc")?;
    /// tree.insert("3", "abc")?;
    /// tree.insert("5", "abc")?;
    ///
    /// let item = tree.first_key_value()?;
    /// assert!(item.is_some());
    /// let item = item.unwrap();
    /// assert_eq!(item.key, "1".as_bytes());
    ///
    /// # Ok::<(), TreeError>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs
    pub fn first_key_value(&self) -> crate::Result<Option<Value>> {
        let item = self.iter()?.into_iter().next().transpose()?;
        Ok(item)
    }

    /* /// Returns the last key-value pair in the LSM-tree. The key in this pair is the maximum key in the LSM-tree
    /// #
    /// # Example usage
    ///
    /// ```
    /// # use lsm_tree::Error as TreeError;
    /// # use lsm_tree::{Tree, Config};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let mut tree = Config::new(folder).open()?;
    /// #
    /// tree.insert("1", "abc")?;
    /// tree.insert("3", "abc")?;
    /// tree.insert("5", "abc")?;
    /// #
    /// let item = tree.last_key_value()?;
    /// assert!(item.is_some());
    /// let item = item.unwrap();
    /// assert_eq!(item.key, "5".as_bytes());
    ///
    /// # Ok::<(), TreeError>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs
    pub fn last_key_value(&self) -> crate::Result<Option<Value>> {
        let item = self.iter()?.into_iter().next_back().transpose()?;
        Ok(item)
    } */

    /// Retrieves an item from the tree
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// use lsm_tree::{Config, Tree};
    ///
    /// let tree = Config::new(folder).open()?;
    /// tree.insert("a", nanoid::nanoid!())?;
    ///
    /// let item = tree.get("a")?;
    /// assert!(item.is_some());
    ///
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs
    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> crate::Result<Option<Value>> {
        let memtable_lock = self.active_memtable.read().expect("lock is poisoned");

        if let Some(item) = memtable_lock.get(&key) {
            return Ok(ignore_tombstone_value(item));
        }
        drop(memtable_lock);

        let memtable_lock = self.immutable_memtables.read().expect("lock is poisoned");
        for (_, memtable) in memtable_lock.iter().rev() {
            if let Some(item) = memtable.get(&key) {
                return Ok(ignore_tombstone_value(item));
            }
        }
        drop(memtable_lock);

        let segment_lock = self.levels.read().expect("lock is poisoned");
        let segments = &segment_lock.get_all_segments_flattened();

        for segment in segments {
            if let Some(item) = segment.get(&key)? {
                return Ok(ignore_tombstone_value(item));
            }
        }

        Ok(None)
    }

    /// Atomically fetches and updates an item if it exists
    ///
    /// Returns the previous value if the item exists
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs
    pub fn fetch_replace<K: AsRef<[u8]>, F: Fn(&[u8]) -> Vec<u8>>(
        &self,
        key: K,
        f: F,
    ) -> crate::Result<Option<Vec<u8>>> {
        // TODO: fully lock all shards

        let commit_log_lock = self.commit_log.lock().expect("lock is poisoned");

        Ok(match self.get(key)? {
            Some(item) => {
                let updated_value = f(&item.value);

                self.append_entry(
                    commit_log_lock,
                    Value {
                        key: item.key,
                        value: updated_value,
                        is_tombstone: false,
                        seqno: self.lsn.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
                    },
                )?;

                Some(item.value)
            }
            None => None,
        })
    }

    /// Atomically fetches and updates an item if it exists
    ///
    /// Returns the updated value if the item exists
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs
    pub fn fetch_update<K: AsRef<[u8]>, F: Fn(&[u8]) -> Vec<u8>>(
        &self,
        key: K,
        f: F,
    ) -> crate::Result<Option<Vec<u8>>> {
        // TODO: fully lock all shards

        let commit_log_lock = self.commit_log.lock().expect("lock is poisoned");

        Ok(match self.get(key)? {
            Some(item) => {
                let updated_value = f(&item.value);

                self.append_entry(
                    commit_log_lock,
                    Value {
                        key: item.key,
                        value: updated_value.clone(),
                        is_tombstone: false,
                        seqno: self.lsn.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
                    },
                )?;

                Some(updated_value)
            }
            None => None,
        })
    }

    /// Force-starts a memtable flush thread
    #[doc(hidden)]
    pub fn force_memtable_flush(
        &self,
    ) -> crate::Result<std::thread::JoinHandle<crate::Result<()>>> {
        let commit_log = self.commit_log.lock().expect("lock is poisoned");
        let memtable = self.active_memtable.write().expect("lock is poisoned");

        crate::flush::start(self, commit_log, memtable)
    }

    /// Force-starts a memtable flush thread and waits until its completely done
    #[doc(hidden)]
    pub fn wait_for_memtable_flush(&self) -> crate::Result<()> {
        let flush_thread = self.force_memtable_flush()?;
        flush_thread.join().expect("should join")
    }

    /// Perform major compaction
    #[doc(hidden)]
    #[must_use]
    pub fn do_major_compaction(&self) -> std::thread::JoinHandle<crate::Result<()>> {
        log::info!("Starting major compaction thread");
        let tree = self.clone();

        std::thread::spawn(move || {
            let levels = tree.levels.write().expect("lock is poisoned");
            let compactor = crate::compaction::major::Strategy::default();

            let choice = compactor.choose(&levels);

            if let crate::compaction::Choice::DoCompact(payload) = choice {
                crate::compaction::worker::do_compaction(&tree, &payload, levels)?;
            }
            Ok(())
        })
    }

    /// Flushes the commit log to disk, making sure all written data is persisted
    /// and crash-safe
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
    ///
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs
    pub fn flush(&self) -> crate::Result<()> {
        let mut lock = self.commit_log.lock().expect("lock is poisoned");
        lock.flush()?;
        Ok(())
    }
}
