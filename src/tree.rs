use crate::{
    block_cache::BlockCache,
    commit_log::CommitLog,
    level::Levels,
    memtable::{recovery::Strategy, MemTable},
    segment::{index::MetaIndex, meta::Metadata, writer::Writer, Segment},
    time::unix_timestamp,
    tree_inner::TreeInner,
    Batch, Config, Value,
};
use std::{
    collections::HashMap,
    fs::create_dir_all,
    path::PathBuf,
    sync::{atomic::AtomicU64, Arc, Mutex, MutexGuard, RwLock, RwLockWriteGuard},
};

/// A log-structured merge tree (LSM tree/LSMT)
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

    /// Returns the folder path used by the tree
    #[must_use]
    pub fn path(&self) -> PathBuf {
        self.config.path.clone()
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
        // TODO: optimize
        self.len().map(|x| x == 0)
    }

    /// Returns the number of items in the tree.
    ///
    /// ## This does a full scan!
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// use lsm_tree::{Config, Tree};
    ///
    /// let tree = Config::new(folder).open()?;
    /// assert!(tree.len()? == 0);
    ///
    /// tree.insert("a", nanoid::nanoid!())?;
    /// assert!(tree.len()? == 1);
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// - Will return `Err` if an IO error occurs
    pub fn len(&self) -> crate::Result<usize> {
        let lock = self.active_memtable.read().expect("should lock");
        // TODO: use iter().count()
        Ok(lock.len())
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

        let inner = TreeInner {
            config,
            active_memtable: Arc::new(RwLock::new(MemTable::default())),
            commit_log: Arc::new(Mutex::new(CommitLog::new(log_path)?)),
            block_cache: Arc::new(BlockCache::new(1024 /* TODO: */)),
            lsn: AtomicU64::new(0),
            levels: Arc::new(RwLock::new(levels)),
        };

        // Create subfolders
        create_dir_all(inner.config.path.join("segments"))?;
        create_dir_all(inner.config.path.join("logs"))?;

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
        let (lsn, _, memtable) =
            MemTable::from_file(config.path.join("log"), &Strategy::default()).unwrap();

        // TODO: read segments etc, get LSN

        let log_path = config.path.join("log");

        let levels = Levels::from_disk(&config.path.join("levels.json"), HashMap::new())?;

        let inner = TreeInner {
            config,
            active_memtable: Arc::new(RwLock::new(memtable)),
            block_cache: Arc::new(BlockCache::new(1024 /* TODO: */)),
            commit_log: Arc::new(Mutex::new(CommitLog::new(log_path)?)),
            lsn: AtomicU64::new(lsn),
            levels: Arc::new(RwLock::new(levels)),
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

        memtable.insert(value, bytes_written);

        // TODO: memtable size param
        if memtable.exceeds_threshold(160_000_000) {
            let segment_id = unix_timestamp().as_micros().to_string();
            let old_commit_log_path = self.config.path.join(format!("logs/{segment_id}"));

            std::fs::rename(self.config.path.join("log"), old_commit_log_path.clone())?;
            *commit_log = CommitLog::new(self.config.path.join("log"))?;
            drop(commit_log);

            let old_memtable = std::mem::take(&mut *memtable);
            drop(memtable);

            let tree_path = self.config.path.clone();
            let segment_folder = self.config.path.join(format!("segments/{segment_id}"));
            let block_size = self.config.block_size;
            let block_cache = Arc::clone(&self.block_cache);
            let levels_manifest = Arc::clone(&self.levels);

            let thread = std::thread::spawn(move || {
                let mut segment_writer = Writer::new(crate::segment::writer::Options {
                    path: segment_folder.clone(),
                    evict_tombstones: false,
                    block_size,
                })?;

                log::debug!(
                    "Flushing memtable -> {}",
                    segment_writer.opts.path.display()
                );

                for (_, value) in old_memtable.items {
                    segment_writer.write(value)?;
                }

                segment_writer.finalize()?;
                log::debug!("Finalized segment write");

                let segment_path_relative =
                    pathdiff::diff_paths(&segment_folder, &tree_path).unwrap();
                let metadata =
                    Metadata::from_writer(segment_id, segment_writer, segment_path_relative);
                metadata.write_to_file(segment_folder.join("meta.json"))?;

                let meta_index = Arc::new(
                    MetaIndex::from_file(&segment_folder, Arc::clone(&block_cache)).expect("TODO:"),
                );

                let created_segment = Segment {
                    block_index: meta_index,
                    block_cache,
                    metadata,
                };

                let mut levels = levels_manifest.write().expect("should lock");
                levels.add(Arc::new(created_segment));
                levels.write_to_disk()?;
                drop(levels);

                log::debug!("Flush done");

                std::fs::remove_file(old_commit_log_path)?;

                Ok::<_, crate::Error>(())
            });
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
        let lock = self.active_memtable.write().expect("should lock");

        if let Some(item) = lock.get(key) {
            if item.is_tombstone {
                Ok(None)
            } else {
                Ok(Some(item))
            }
        } else {
            Ok(None)
        }

        // TODO: walk segments
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
