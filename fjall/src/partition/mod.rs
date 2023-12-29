pub mod config;
pub mod name;

use crate::{
    file::PARTITIONS_FOLDER, flush::manager::Task as FlushTask, journal::manager::PartitionSeqNo,
    Keyspace,
};
use config::Config;
use lsm_tree::{
    compaction::CompactionStrategy, prefix::Prefix, range::Range, Tree as LsmTree, UserKey,
    UserValue,
};
use std::{ops::RangeBounds, path::PathBuf, sync::Arc, time::Duration};

#[allow(clippy::module_name_repetitions)]
pub struct PartitionHandleInner {
    /// Partition name
    pub name: Arc<str>,

    pub(crate) keyspace: Keyspace,

    /// TEMP pub
    pub(crate) tree: LsmTree,

    /// Maximum size of this partition's memtable
    pub(crate) max_memtable_size: u32, // TODO: make editable

    pub(crate) compaction_strategy: Arc<dyn CompactionStrategy + Send + Sync>,
    // TODO: need a destroyed flag or something, so GC can check the partition is deleted
    // TODO: handle mid-flush deletes of partition folders... (don't delete folder immediately)
    // TODO: just attach marker to folder and let GC handle it, when no flushes are running
}

/// Access to a keyspace partition.partition
#[derive(Clone)]
#[allow(clippy::module_name_repetitions)]
pub struct PartitionHandle(pub(crate) Arc<PartitionHandleInner>);

impl std::ops::Deref for PartitionHandle {
    type Target = PartitionHandleInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl PartitionHandle {
    /// Creates a new partition
    pub(crate) fn create_new(
        keyspace: Keyspace,
        name: Arc<str>,
        config: Config,
    ) -> crate::Result<Self> {
        log::debug!("Creating partition {name}");

        let path = keyspace.config.path.join(PARTITIONS_FOLDER).join(&*name);

        let tree = lsm_tree::Config::new(path)
            .block_cache(keyspace.config.block_cache.clone())
            .block_size(config.block_size)
            .level_count(config.level_count)
            .level_ratio(config.level_ratio)
            .open()?;

        Ok(Self(Arc::new(PartitionHandleInner {
            name,
            keyspace,
            tree,
            compaction_strategy: config.compaction_strategy,
            max_memtable_size: config.max_memtable_size,
        })))
    }

    /// Returns the underlying LSM-tree's path
    #[must_use]
    pub fn path(&self) -> PathBuf {
        self.tree.config.path.clone()
    }

    /// Returns the disk space usage of this partition
    #[must_use]
    pub fn disk_space(&self) -> u64 {
        self.tree.disk_space()
    }

    /// Destroys the partition, removing all data associated with it.
    pub fn destroy(self) -> crate::Result<()> {
        // TODO: this needs to be atomic...
        // TODO: remove from partitions
        // TODO: delete folder...
        Ok(())
    }

    #[allow(clippy::iter_not_returning_iterator)]
    /// Returns an iterator that scans through the entire partition.
    ///
    /// Avoid using this function, or limit it as otherwise it may scan a lot of items.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionConfig};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open()?;
    /// # let partition = keyspace.open_partition("default", PartitionConfig::default())?;
    /// partition.insert("a", "abc")?;
    /// partition.insert("f", "abc")?;
    /// partition.insert("g", "abc")?;
    /// assert_eq!(3, partition.iter().into_iter().count());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    #[must_use]
    pub fn iter(&self) -> Range {
        self.tree.iter()
    }

    /// Returns an iterator over a range of items.
    ///
    /// Avoid using full or unbounded ranges as they may scan a lot of items (unless limited).
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionConfig};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open()?;
    /// # let partition = keyspace.open_partition("default", PartitionConfig::default())?;
    /// partition.insert("a", "abc")?;
    /// partition.insert("f", "abc")?;
    /// partition.insert("g", "abc")?;
    /// assert_eq!(2, partition.range("a"..="f").into_iter().count());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn range<K: AsRef<[u8]>, R: RangeBounds<K>>(&self, range: R) -> Range {
        self.tree.range(range)
    }

    /// Returns an iterator over a prefixed set of items.
    ///
    /// Avoid using an empty prefix as it may scan a lot of items (unless limited).
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionConfig};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open()?;
    /// # let partition = keyspace.open_partition("default", PartitionConfig::default())?;
    /// partition.insert("a", "abc")?;
    /// partition.insert("ab", "abc")?;
    /// partition.insert("abc", "abc")?;
    /// assert_eq!(2, partition.prefix("ab").into_iter().count());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn prefix<K: AsRef<[u8]>>(&self, prefix: K) -> Prefix {
        self.tree.prefix(prefix)
    }

    /// Scans the entire partition, returning the amount of items.
    ///
    /// ###### Caution
    ///
    /// This operation scans the entire partition: O(n) complexity!
    ///
    /// Never, under any circumstances, use .len() == 0 to check
    /// if the partition is empty, use [`PartitionHandle::is_empty`] instead.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionConfig};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open()?;
    /// # let partition = keyspace.open_partition("default", PartitionConfig::default())?;
    /// assert_eq!(partition.len()?, 0);
    /// partition.insert("1", "abc")?;
    /// partition.insert("3", "abc")?;
    /// partition.insert("5", "abc")?;
    /// assert_eq!(partition.len()?, 3);
    /// #
    /// # Ok::<(), fjall::Error>(())
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

    /// Returns `true` if the partition is empty.
    ///
    /// This operation has O(1) complexity.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionConfig};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open()?;
    /// # let partition = keyspace.open_partition("default", PartitionConfig::default())?;
    /// assert!(partition.is_empty()?);
    ///
    /// partition.insert("a", "abc")?;
    /// assert!(!partition.is_empty()?);
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

    /// Returns `true` if the partition contains the specified key.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionConfig};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open()?;
    /// # let partition = keyspace.open_partition("default", PartitionConfig::default())?;
    /// assert!(!partition.contains_key("a")?);
    ///
    /// partition.insert("a", "abc")?;
    /// assert!(partition.contains_key("a")?);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn contains_key<K: AsRef<[u8]>>(&self, key: K) -> crate::Result<bool> {
        self.get(key).map(|x| x.is_some())
    }

    /// Retrieves an item from the partition.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionConfig};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open()?;
    /// # let partition = keyspace.open_partition("default", PartitionConfig::default())?;
    /// partition.insert("a", "my_value")?;
    ///
    /// let item = partition.get("a")?;
    /// assert_eq!(Some("my_value".as_bytes().into()), item);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> crate::Result<Option<lsm_tree::UserValue>> {
        Ok(self.tree.get(key)?)
    }

    /// Returns the first key-value pair in the partition.
    /// The key in this pair is the minimum key in the partition.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionConfig};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open()?;
    /// # let partition = keyspace.open_partition("default", PartitionConfig::default())?;
    /// partition.insert("1", "abc")?;
    /// partition.insert("3", "abc")?;
    /// partition.insert("5", "abc")?;
    ///
    /// let (key, _) = partition.first_key_value()?.expect("item should exist");
    /// assert_eq!(&*key, "1".as_bytes());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn first_key_value(&self) -> crate::Result<Option<(UserKey, UserValue)>> {
        Ok(self.tree.first_key_value()?)
    }

    /// Returns the last key-value pair in the partition.
    /// The key in this pair is the maximum key in the partition.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionConfig};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open()?;
    /// # let partition = keyspace.open_partition("default", PartitionConfig::default())?;
    /// partition.insert("1", "abc")?;
    /// partition.insert("3", "abc")?;
    /// partition.insert("5", "abc")?;
    ///
    /// let (key, _) = partition.last_key_value()?.expect("item should exist");
    /// assert_eq!(&*key, "5".as_bytes());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn last_key_value(&self) -> crate::Result<Option<(UserKey, UserValue)>> {
        Ok(self.tree.last_key_value()?)
    }

    fn rotate_memtable(&self) -> crate::Result<()> {
        // Rotate memtable
        let Some((yanked_id, yanked_memtable)) = self.tree.rotate_memtable() else {
            log::debug!("Got no sealed memtable, someone beat us to it");
            return Ok(());
        };

        log::debug!("partition: acquiring journal manager lock");
        let mut journal_manager = self
            .keyspace
            .journal_manager
            .write()
            .expect("lock is poisoned");

        log::trace!("partition: writing partition seqno");
        let memtable_max_seqno = yanked_memtable
            .get_lsn()
            .expect("sealed memtable is never empty");

        journal_manager.rotate_journal(
            std::iter::once((
                self.name.clone(),
                PartitionSeqNo {
                    lsn: memtable_max_seqno,
                    partition: self.clone(),
                },
            ))
            .collect(),
        )?;

        log::debug!("partition: acquiring flush manager lock");
        let mut flush_manager = self
            .keyspace
            .flush_manager
            .write()
            .expect("lock is poisoned");

        flush_manager.enqueue_task(
            self.name.clone(),
            FlushTask {
                id: yanked_id,
                partition: self.clone(),
                sealed_memtable: yanked_memtable,
            },
        );

        let journal_size = journal_manager.disk_space_used();
        drop(journal_manager);
        drop(flush_manager);

        // Notify flush worker that new work has arrived
        self.keyspace.flush_semaphore.release();

        if journal_size > self.keyspace.config.max_journaling_size_in_bytes.into() {
            // TODO: maybe exponential backoff

            loop {
                log::warn!("Too many journals amassed, halting writes...");

                self.keyspace.flush_semaphore.release();
                std::thread::sleep(Duration::from_millis(1_000));

                let bytes = self
                    .keyspace
                    .journal_manager
                    .read()
                    .expect("lock is poisoned")
                    .disk_space_used();

                if bytes <= self.keyspace.config.max_journaling_size_in_bytes.into() {
                    log::debug!("Ending write halt");
                    break;
                }
            }
        }

        Ok(())
    }

    fn check_write_stall(&self) {
        while self.tree.first_level_segment_count() > 20 {
            log::warn!("Halting writes until L0 is cleared up...");
            self.keyspace.compaction_manager.notify(self.clone());
            std::thread::sleep(Duration::from_millis(1_000));
        }
    }

    /// Inserts a key-value pair into the partition.
    ///
    /// Keys may be up to 65536 bytes long, values up to 2^32 bytes.
    /// Shorter keys and values result in better performance.
    ///
    /// If the key already exists, the item will be overwritten.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionConfig};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open()?;
    /// # let partition = keyspace.open_partition("default", PartitionConfig::default())?;
    /// partition.insert("a", "abc")?;
    ///
    /// assert!(!partition.is_empty()?);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn insert<K: AsRef<[u8]>, V: AsRef<[u8]>>(&self, key: K, value: V) -> crate::Result<()> {
        let mut shard = self.keyspace.journal.get_writer();

        let seqno = self.keyspace.seqno.next();

        shard.writer.write(
            &crate::batch::Item {
                key: key.as_ref().into(),
                value: value.as_ref().into(),
                partition: self.name.clone(),
                value_type: lsm_tree::ValueType::Value,
            },
            seqno,
        )?;
        drop(shard);

        let memtable_size = self.tree.insert(key, value, seqno);

        if memtable_size > self.max_memtable_size {
            log::debug!("insert: rotating memtable");
            self.rotate_memtable()?;
            self.check_write_stall();
        }

        if self.tree.first_level_segment_count() > 16 {
            log::info!("Stalling writes...");
            self.keyspace.compaction_manager.notify(self.clone());
            std::thread::sleep(Duration::from_millis(1_000));
        }

        Ok(())
    }

    /// Removes an item from the partition.
    ///
    /// The key may be up to 65536 bytes long.
    /// Shorter keys result in better performance.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionConfig};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open()?;
    /// # let partition = keyspace.open_partition("default", PartitionConfig::default())?;
    /// partition.insert("a", "abc")?;
    ///
    /// let item = partition.get("a")?.expect("should have item");
    /// assert_eq!("abc".as_bytes(), &*item);
    ///
    /// partition.remove("a")?;
    ///
    /// let item = partition.get("a")?;
    /// assert_eq!(None, item);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn remove<K: AsRef<[u8]>>(&self, key: K) -> crate::Result<()> {
        let mut shard = self.keyspace.journal.get_writer();

        let seqno = self.keyspace.seqno.next();

        shard.writer.write(
            &crate::batch::Item {
                key: key.as_ref().into(),
                value: [].into(),
                partition: self.name.clone(),
                value_type: lsm_tree::ValueType::Tombstone,
            },
            seqno,
        )?;
        drop(shard);

        let memtable_size = self.tree.remove(key, seqno);

        if memtable_size > self.max_memtable_size {
            log::debug!("remove: rotating memtable");
            self.check_write_stall();
            self.rotate_memtable()?;
        }

        if self.tree.first_level_segment_count() > 16 {
            log::info!("Stalling writes...");
            self.keyspace.compaction_manager.notify(self.clone());
            std::thread::sleep(Duration::from_millis(1_000));
        }

        Ok(())
    }
}
