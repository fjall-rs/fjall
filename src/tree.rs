use crate::{
    block_cache::BlockCache,
    commit_log::CommitLog,
    memtable::{recovery::Strategy, MemTable},
    tree_inner::TreeInner,
    Batch, Config, Value,
};
use std::sync::{atomic::AtomicU64, Arc};

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
    /// batch.insert("d", "idontlikeu");
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
        // TODO:
        Ok(self.active_memtable.len())
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

        let inner = TreeInner {
            config,
            active_memtable: Arc::new(MemTable::default()),
            commit_log: Arc::new(CommitLog::new(log_path)?),
            block_cache: Arc::new(BlockCache::new(1024 /* TODO: */)),
            lsn: AtomicU64::new(0),
        };

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

        let inner = TreeInner {
            config,
            active_memtable: Arc::new(memtable),
            block_cache: Arc::new(BlockCache::new(1024 /* TODO: */)),
            commit_log: Arc::new(CommitLog::new(log_path)?),
            lsn: AtomicU64::new(lsn),
        };

        Ok(Self(Arc::new(inner)))
    }

    // TODO: open (recover.or_else(create_new))

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
        let value = Value::new(
            key,
            value,
            false,
            self.lsn.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
        );

        // TODO: make sure if someone writes to log, they also get to write to the memtable
        // next
        self.commit_log.append(value.clone())?;
        self.active_memtable.insert(value, 0 /* TODO: */);

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
        let value = Value::new(
            key,
            vec![],
            true,
            self.lsn.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
        );

        let key = value.key.clone();

        self.commit_log.append(value)?;
        self.active_memtable.remove(&key);

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
        Ok(self.active_memtable.get(key))

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
        self.commit_log.flush()?;

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
