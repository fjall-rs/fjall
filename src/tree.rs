use std_semaphore::Semaphore;

use crate::{
    block_cache::BlockCache,
    commit_log::CommitLog,
    level::Levels,
    memtable::{recovery::Strategy, MemTable},
    prefix::Prefix,
    range::{MemTableGuard, Range},
    tree_inner::TreeInner,
    Batch, Config, Value,
};
use std::{
    collections::HashMap,
    ops::RangeBounds,
    path::PathBuf,
    sync::{atomic::AtomicU64, Arc, Mutex, MutexGuard, RwLock},
};

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
        let levels = self.levels.read().expect("should lock");
        levels.is_compacting()
    }

    /// Counts the amount of segments currently in the tree
    #[must_use]
    pub fn segment_count(&self) -> usize {
        self.levels.read().expect("should lock").len()
    }

    /// Sums the disk space usage of the tree (segments + commit log)
    #[must_use]
    pub fn disk_space(&self) -> u64 {
        let segment_size: u64 = self
            .levels
            .read()
            .expect("should lock")
            .get_all_segments()
            .values()
            .map(|x| x.metadata.file_size)
            .sum();

        let memtable = self.active_memtable.read().expect("should lock");

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
    /// Will return `Err` if an IO error happens
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

        let file = std::fs::File::create(marker)?;
        file.sync_all()?;

        let log_path = config.path.join("log");
        let levels = Levels::create_new(config.levels, config.path.join("levels.json"))?;

        let block_cache = Arc::new(BlockCache::new(config.block_cache_size as usize));

        let inner = TreeInner {
            config,
            active_memtable: Arc::new(RwLock::new(MemTable::default())),
            immutable_memtables: Arc::default(),
            commit_log: Arc::new(Mutex::new(CommitLog::new(log_path)?)),
            block_cache,
            lsn: AtomicU64::new(0),
            levels: Arc::new(RwLock::new(levels)),
            flush_semaphore: Arc::new(Semaphore::new(4)), // TODO: config
        };

        // Create subfolders
        std::fs::create_dir_all(inner.config.path.join("segments"))?;
        std::fs::create_dir_all(inner.config.path.join("logs"))?;

        // Fsync folder
        let folder = std::fs::File::open(&inner.config.path)?;
        folder.sync_all()?;

        Ok(Self(Arc::new(inner)))
    }

    /// Tries to recover a tree from a folder.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs
    fn recover(config: Config) -> crate::Result<Self> {
        let (lsn, bytes_written, memtable) =
            MemTable::from_file(config.path.join("log"), &Strategy::default()).unwrap();

        dbg!("Loaded memtable with # entries:", memtable.len());
        dbg!("Commit Log size", bytes_written);

        // TODO: read segments etc, get LSN

        let log_path = config.path.join("log");

        let levels = Levels::from_disk(&config.path.join("levels.json"), HashMap::new())?;

        let block_cache = Arc::new(BlockCache::new(config.block_cache_size as usize));

        let inner = TreeInner {
            config,
            active_memtable: Arc::new(RwLock::new(memtable)),
            immutable_memtables: Arc::default(),
            block_cache,
            commit_log: Arc::new(Mutex::new(CommitLog::new(log_path)?)),
            lsn: AtomicU64::new(lsn),
            levels: Arc::new(RwLock::new(levels)),
            flush_semaphore: Arc::new(Semaphore::new(4)), // TODO: config
        };

        Ok(Self(Arc::new(inner)))
    }

    fn append_entry(
        &self,
        mut commit_log: MutexGuard<CommitLog>,
        value: Value,
    ) -> crate::Result<()> {
        let mut memtable = self.active_memtable.write().expect("should lock");

        //let mut lock = self.commit_log.lock().expect("should lock");
        let bytes_written = commit_log.append(value.clone())?;

        memtable.insert(value, bytes_written as u32);

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
        /* let start = std::time::Instant::now(); */
        // let mut lock = self.active_memtable.write().expect("should lock");
        let commit_log = self.commit_log.lock().expect("should lock");
        /* eprintln!("locked commit log in {}ns", start.elapsed().as_nanos()); */

        let value = Value::new(
            key,
            value,
            false,
            self.lsn.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
        );

        /* let start = std::time::Instant::now(); */
        self.append_entry(commit_log, value)?;
        /* eprintln!("appended entry in {}ns", start.elapsed().as_nanos()); */

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
        let commit_log = self.commit_log.lock().expect("should lock");

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
    /// # Errors
    ///
    /// Will return `Err` if an IO error happens
    pub fn iter(&self) -> crate::Result<Range<'_>> {
        self.range::<Vec<u8>, _>(..)
    }

    /// Returns an iterator over a range of items
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error happens
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
            .collect();

        Ok(Range::new(
            crate::range::MemTableGuard {
                active: self.active_memtable.read().expect("should lock"),
                immutable: self.immutable_memtables.read().expect("should lock"),
            },
            bounds,
            segment_info,
            Arc::clone(&self.block_cache),
        ))
    }

    /// Returns an iterator over a prefixed set of items
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error happens
    pub fn prefix<K: AsRef<[u8]>>(&self, prefix: &K) -> std::io::Result<Prefix<'_>> {
        use std::ops::Bound::{self};

        let prefix = prefix.as_ref();

        let lock = self.levels.read().expect("lock poisoned");

        let bounds: (Bound<Vec<u8>>, Bound<Vec<u8>>) =
            (Bound::Included(prefix.into()), std::ops::Bound::Unbounded);

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
            prefix.into(),
            segment_info,
            Arc::clone(&self.block_cache),
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

    /// Returns the last key-value pair in the LSM-tree. The key in this pair is the maximum key in the LSM-tree
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
    }

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
        let lock = self.active_memtable.read().expect("should lock");

        if let Some(item) = lock.get(&key) {
            return if item.is_tombstone {
                Ok(None)
            } else {
                Ok(Some(item))
            };
        }

        let memtable_lock = self.immutable_memtables.read().expect("should lock");
        for (_, memtable) in memtable_lock.iter().rev() {
            if let Some(item) = memtable.get(&key) {
                return if item.is_tombstone {
                    Ok(None)
                } else {
                    Ok(Some(item))
                };
            }
        }
        drop(memtable_lock);

        let segment_lock = self.levels.read().expect("should lock");
        let segments = &segment_lock.get_all_segments_flattened();

        for segment in segments {
            let item = segment.get(&key)?;

            if let Some(ref entry) = item {
                return if entry.is_tombstone {
                    Ok(None)
                } else {
                    Ok(item)
                };
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

        let commit_log_lock = self.commit_log.lock().expect("should lock");

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

        let commit_log_lock = self.commit_log.lock().expect("should lock");

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
        let commit_log = self.commit_log.lock().expect("should lock");
        let memtable = self.active_memtable.write().expect("should lock");

        crate::flush::start(self, commit_log, memtable)
    }

    /// Force-starts a memtable flush thread and waits until its completely done
    #[doc(hidden)]
    pub fn wait_for_memtable_flush(&self) -> crate::Result<()> {
        let flush_thread = self.force_memtable_flush()?;
        flush_thread.join().expect("should join")
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
        let mut lock = self.commit_log.lock().expect("should lock");
        lock.flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_log::test;

    #[test]
    fn test_write_many() {
        const ITEM_COUNT: usize = 4_000;

        for thread_count in [1, 2, 4, 8, 16] {
            let folder = tempfile::tempdir().unwrap().into_path();
            let tree = Config::new(folder).open().unwrap();

            let start = std::time::Instant::now();

            let threads = (0..thread_count)
                .map(|thread_no| {
                    let tree = tree.clone();

                    std::thread::spawn(move || {
                        let item_count = ITEM_COUNT / thread_count;
                        let start = thread_no * item_count;
                        let range = start..(start + item_count);

                        for key in range {
                            tree.insert((key as u64).to_be_bytes(), nanoid::nanoid!())
                                .unwrap();
                        }
                    })
                })
                .collect::<Vec<_>>();

            for thread in threads {
                thread.join().unwrap();
            }

            tree.flush().unwrap();

            let elapsed = start.elapsed();
            let nanos = elapsed.as_nanos();
            let nanos_per_item = nanos / ITEM_COUNT as u128;
            let reads_per_second = (std::time::Duration::from_secs(1)).as_nanos() / nanos_per_item;

            eprintln!(
                "done in {:?}s, {}ns per item - {} RPS",
                elapsed.as_secs_f64(),
                nanos_per_item,
                reads_per_second
            );

            let start = std::time::Instant::now();

            for x in 0..ITEM_COUNT {
                let item = tree.get((x as u64).to_be_bytes()).unwrap().unwrap();
                assert_eq!(item.key, (x as u64).to_be_bytes());
                assert!(!item.is_tombstone);
                //assert_eq!(item.seqno, x as u64);
            }

            let elapsed = start.elapsed();
            let nanos = elapsed.as_nanos();
            let nanos_per_item = nanos / ITEM_COUNT as u128;
            let reads_per_second = (std::time::Duration::from_secs(1)).as_nanos() / nanos_per_item;

            eprintln!(
                "done in {:?}s, {}ns per item - {} RPS",
                elapsed.as_secs_f64(),
                nanos_per_item,
                reads_per_second
            );
        }
    }

    #[test]
    fn test_write_and_read() {
        let folder = tempfile::tempdir().unwrap().into_path();

        let tree = Config::new(folder.clone()).open().unwrap();

        tree.insert("a", nanoid::nanoid!()).unwrap();
        tree.insert("b", nanoid::nanoid!()).unwrap();
        tree.insert("c", nanoid::nanoid!()).unwrap();
        tree.flush().unwrap();

        let item = tree.get("a").unwrap().unwrap();
        assert_eq!(item.key, b"a");
        assert!(!item.is_tombstone);
        assert_eq!(item.seqno, 0);

        let item = tree.get("b").unwrap().unwrap();
        assert_eq!(item.key, b"b");
        assert!(!item.is_tombstone);
        assert_eq!(item.seqno, 1);

        let item = tree.get("c").unwrap().unwrap();
        assert_eq!(item.key, b"c");
        assert!(!item.is_tombstone);
        assert_eq!(item.seqno, 2);

        let tree = Config::new(folder).open().unwrap();

        let item = tree.get("a").unwrap().unwrap();
        assert_eq!(item.key, b"a");
        assert!(!item.is_tombstone);
        assert_eq!(item.seqno, 0);

        let item = tree.get("b").unwrap().unwrap();
        assert_eq!(item.key, b"b");
        assert!(!item.is_tombstone);
        assert_eq!(item.seqno, 1);

        let item = tree.get("c").unwrap().unwrap();
        assert_eq!(item.key, b"c");
        assert!(!item.is_tombstone);
        assert_eq!(item.seqno, 2);
    }
}
