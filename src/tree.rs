use crate::{
    block_cache::BlockCache,
    commit_log::CommitLog,
    memtable::{recovery::Strategy, MemTable},
    tree_inner::TreeInner,
    Config, Value,
};
use std::sync::{atomic::AtomicU64, Arc};

/// A log-structured merge tree (LSM tree/LSMT)
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
    /// Will create a new tree if the folder does not exist or recover a previous state
    /// if it exists
    ///
    /// # Errors
    ///
    /// - Will return `Err` if an IO error occurs
    pub fn open(config: Config) -> crate::Result<Self> {
        if config.path.exists() {
            Self::recover(config)
        } else {
            Self::create_new(config)
        }
    }

    /// Creates a new tree in a folder.
    ///
    /// # Errors
    ///
    /// - Will return `Err` if an IO error occurs
    /// - Will fail, if the folder already exists
    fn create_new(config: Config) -> crate::Result<Self> {
        assert!(!config.path.exists());

        let log_path = config.path.join("log");

        let inner = TreeInner {
            config,
            active_memtable: Arc::new(MemTable::default()),
            commit_log: Arc::new(CommitLog::new(log_path)?),
            block_cache: Arc::new(BlockCache::new(1024 /* TODO: */)),
            lsn: AtomicU64::new(0),
        };

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
    /// use lsm_tree::Tree;
    ///
    /// # // TODO: use Config::open() instead
    /// let tree = Tree::create_new(folder, /* TODO: use open instead */ 0);
    /// tree.insert("a", nanoid::nanoid!()).unwrap();
    ///
    /// # Ok::<(), std::io::Error>(())
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

        self.commit_log.append(value.clone())?;
        self.active_memtable.insert(value, 0);

        Ok(())
    }

    /// Returns `true` if the tree contains the specified key
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs
    pub fn contains<K: AsRef<[u8]>>(&self, key: K) -> crate::Result<bool> {
        self.get(key).map(|x| x.is_some())
    }

    /// Retrieves an item from the tree
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs
    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> crate::Result<Option<Value>> {
        Ok(self.active_memtable.get(key))
    }

    /// Flushes the commit log to disk, making sure all written data is persisted
    /// and crash-safe
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

            eprintln!("flushing commit log");
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
        eprintln!("flushing commit log");
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
