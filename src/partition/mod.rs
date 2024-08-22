// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

pub mod config;
pub mod name;
mod write_delay;

use crate::{
    batch::{item::Item as BatchItem, PartitionKey},
    compaction::manager::CompactionManager,
    config::Config as KeyspaceConfig,
    file::{PARTITIONS_FOLDER, PARTITION_DELETED_MARKER},
    flush::manager::{FlushManager, Task as FlushTask},
    journal::{
        manager::{JournalManager, PartitionSeqNo},
        Journal,
    },
    keyspace::Partitions,
    snapshot_nonce::SnapshotNonce,
    snapshot_tracker::SnapshotTracker,
    write_buffer_manager::WriteBufferManager,
    Error, HashMap, Keyspace,
};
use config::CreateOptions;
use lsm_tree::{
    compaction::CompactionStrategy, AbstractTree, AnyTree, KvPair, SequenceNumberCounter, UserKey,
    UserValue,
};
use std::{
    ops::RangeBounds,
    path::Path,
    sync::{
        atomic::{AtomicBool, AtomicU32},
        Arc, RwLock,
    },
    time::Duration,
};
use std_semaphore::Semaphore;
use write_delay::get_write_delay;

pub const DEFAULT_MEMTABLE_SIZE: u32 = /* 16 MiB */ 16 * 1_024 * 1_024;

#[allow(clippy::module_name_repetitions)]
pub struct PartitionHandleInner {
    // Internal
    //
    /// Partition name
    pub name: PartitionKey,

    /// If `true`, the partition is marked as deleted
    pub(crate) is_deleted: AtomicBool,

    /// If `true`, fsync failed during persisting, see `Error::Poisoned`
    pub(crate) is_poisoned: Arc<AtomicBool>,

    /// LSM-tree wrapper
    #[doc(hidden)]
    pub tree: AnyTree,

    // Keyspace stuff
    //
    /// Config of keyspace
    pub(crate) keyspace_config: KeyspaceConfig,

    /// Flush manager of keyspace
    pub(crate) flush_manager: Arc<RwLock<FlushManager>>,

    /// Journal manager of keyspace
    pub(crate) journal_manager: Arc<RwLock<JournalManager>>,

    /// Compaction manager of keyspace
    pub(crate) compaction_manager: CompactionManager,

    /// Write buffer manager of keyspace
    pub(crate) write_buffer_manager: WriteBufferManager,

    // TODO: notifying flush worker should probably become a method in FlushManager
    /// Flush semaphore of keyspace
    pub(crate) flush_semaphore: Arc<Semaphore>,

    /// Journal of keyspace
    pub(crate) journal: Arc<Journal>,

    /// Partition map of keyspace
    pub(crate) partitions: Arc<RwLock<Partitions>>,

    /// Sequence number generator of keyspace
    #[doc(hidden)]
    pub seqno: SequenceNumberCounter,

    // Configuration
    //
    /// Maximum size of this partition's memtable - can be changed during runtime
    pub(crate) max_memtable_size: AtomicU32,

    /// Chosen compaction strategy - can be changed during runtime
    pub(crate) compaction_strategy: RwLock<Arc<dyn CompactionStrategy + Send + Sync>>,

    /// Snapshot tracker
    pub(crate) snapshot_tracker: SnapshotTracker,
}

impl Drop for PartitionHandleInner {
    fn drop(&mut self) {
        log::trace!("Dropping partition inner: {:?}", self.name);

        if self.is_deleted.load(std::sync::atomic::Ordering::Acquire) {
            let path = &self.tree.tree_config().path;

            if let Err(e) = std::fs::remove_dir_all(path) {
                log::error!("Failed to cleanup deleted partition's folder at {path:?}: {e}");
            }
        }

        #[cfg(feature = "__internal_integration")]
        crate::drop::decrement_drop_counter();
    }
}

/// Access to a keyspace partition
///
/// Each partition is backed by an LSM-tree to provide a
/// disk-backed search tree, and can be configured individually.
///
/// A partition generally only takes a little bit of memory and disk space,
/// but does not spawn its own background threads.
#[derive(Clone)]
#[allow(clippy::module_name_repetitions)]
#[doc(alias = "column family")]
#[doc(alias = "locality group")]
#[doc(alias = "table")]
pub struct PartitionHandle(pub(crate) Arc<PartitionHandleInner>);

impl std::ops::Deref for PartitionHandle {
    type Target = PartitionHandleInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl PartialEq for PartitionHandle {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for PartitionHandle {}

impl std::hash::Hash for PartitionHandle {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        state.write(self.name.as_bytes());
    }
}

impl PartitionHandle {
    /// Sets the compaction strategy.
    ///
    /// Default = Leveled
    pub fn set_compaction_strategy(&self, strategy: Arc<dyn CompactionStrategy + Send + Sync>) {
        let mut lock = self.compaction_strategy.write().expect("lock is poisoned");
        *lock = strategy;
    }

    /// Sets the maximum memtable size.
    ///
    /// Default = 8 MiB
    ///
    /// Recommended size 8 - 64 MiB, depending on how much memory
    /// is available.
    /// Note that the memory usage may temporarily be `max_memtable_size * flush_worker_count`
    /// because of parallel flushing.
    /// Use the keyspace's `max_write_buffer_size` to cap global memory usage.
    pub fn set_max_memtable_size(&self, bytes: u32) {
        use std::sync::atomic::Ordering::Release;

        if bytes < 1_000_000 {
            log::warn!("Memtable size below 1 MB is not recommended");
        }

        self.max_memtable_size.store(bytes, Release);
    }

    /// Creates a new partition.
    pub(crate) fn create_new(
        keyspace: &Keyspace,
        name: PartitionKey,
        config: CreateOptions,
    ) -> crate::Result<Self> {
        log::debug!("Creating partition {name:?}");

        let path = keyspace.config.path.join(PARTITIONS_FOLDER).join(&*name);

        if path.join(PARTITION_DELETED_MARKER).exists() {
            log::error!("Failed to open partition, partition is deleted.");
            return Err(Error::PartitionDeleted);
        }

        let base_config = lsm_tree::Config::new(path)
            .descriptor_table(keyspace.config.descriptor_table.clone())
            .block_cache(keyspace.config.block_cache.clone())
            .blob_cache(keyspace.config.blob_cache.clone())
            .block_size(config.block_size)
            .level_count(config.level_count)
            .compression(config.compression);

        let tree = match config.tree_type {
            lsm_tree::TreeType::Standard => AnyTree::Standard(base_config.open()?),
            lsm_tree::TreeType::Blob => AnyTree::Blob(base_config.open_as_blob_tree()?),
        };

        Ok(Self(Arc::new(PartitionHandleInner {
            name,
            partitions: keyspace.partitions.clone(),
            keyspace_config: keyspace.config.clone(),
            flush_manager: keyspace.flush_manager.clone(),
            flush_semaphore: keyspace.flush_semaphore.clone(),
            journal_manager: keyspace.journal_manager.clone(),
            journal: keyspace.journal.clone(),
            compaction_manager: keyspace.compaction_manager.clone(),
            seqno: keyspace.seqno.clone(),
            tree,
            compaction_strategy: RwLock::new(Arc::new(super::compaction::Leveled::default())),
            max_memtable_size: DEFAULT_MEMTABLE_SIZE.into(),
            write_buffer_manager: keyspace.write_buffer_manager.clone(),
            is_deleted: AtomicBool::default(),
            is_poisoned: keyspace.is_poisoned.clone(),
            snapshot_tracker: keyspace.snapshot_tracker.clone(),
        })))
    }

    /// Returns the underlying LSM-tree's path.
    #[must_use]
    pub fn path(&self) -> &Path {
        self.tree.tree_config().path.as_path()
    }

    /// Returns the disk space usage of this partition.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open()?;
    /// # let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
    /// assert_eq!(0, partition.disk_space());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    #[must_use]
    pub fn disk_space(&self) -> u64 {
        self.tree.disk_space()
    }

    /// Returns an iterator that scans through the entire partition.
    ///
    /// Avoid using this function, or limit it as otherwise it may scan a lot of items.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open()?;
    /// # let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
    /// partition.insert("a", "abc")?;
    /// partition.insert("f", "abc")?;
    /// partition.insert("g", "abc")?;
    /// assert_eq!(3, partition.iter().count());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    #[must_use]
    pub fn iter(&self) -> impl DoubleEndedIterator<Item = crate::Result<KvPair>> {
        self.tree.iter().map(|item| item.map_err(Into::into))
    }

    /// Returns an iterator that scans through the entire partition, returning only keys.
    ///
    /// Avoid using this function, or limit it as otherwise it may scan a lot of items.
    #[must_use]
    pub fn keys(&self) -> impl DoubleEndedIterator<Item = crate::Result<UserKey>> {
        self.tree.keys().map(|item| item.map_err(Into::into))
    }

    /// Returns an iterator that scans through the entire partition, returning only values.
    ///
    /// Avoid using this function, or limit it as otherwise it may scan a lot of items.
    #[must_use]
    pub fn values(&self) -> impl DoubleEndedIterator<Item = crate::Result<UserValue>> + 'static {
        self.tree.values().map(|item| item.map_err(Into::into))
    }

    /// Returns an iterator over a range of items.
    ///
    /// Avoid using full or unbounded ranges as they may scan a lot of items (unless limited).
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open()?;
    /// # let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
    /// partition.insert("a", "abc")?;
    /// partition.insert("f", "abc")?;
    /// partition.insert("g", "abc")?;
    /// assert_eq!(2, partition.range("a"..="f").count());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    pub fn range<'a, K: AsRef<[u8]> + 'a, R: RangeBounds<K> + 'a>(
        &'a self,
        range: R,
    ) -> impl DoubleEndedIterator<Item = crate::Result<KvPair>> + 'static {
        self.tree.range(range).map(|item| item.map_err(Into::into))
    }

    /// Returns an iterator over a prefixed set of items.
    ///
    /// Avoid using an empty prefix as it may scan a lot of items (unless limited).
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open()?;
    /// # let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
    /// partition.insert("a", "abc")?;
    /// partition.insert("ab", "abc")?;
    /// partition.insert("abc", "abc")?;
    /// assert_eq!(2, partition.prefix("ab").count());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    pub fn prefix<'a, K: AsRef<[u8]> + 'a>(
        &'a self,
        prefix: K,
    ) -> impl DoubleEndedIterator<Item = crate::Result<KvPair>> + 'static {
        self.tree
            .prefix(prefix)
            .map(|item| item.map_err(Into::into))
    }

    /// Approximates the amount of items in the partition.
    ///
    /// For update -or delete-heavy workloads, this value will
    /// diverge from the real value, but is a O(1) operation.
    ///
    /// For insert-only workloads (e.g. logs, time series)
    /// this value is reliable.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open()?;
    /// # let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
    /// assert_eq!(partition.len()?, 0);
    ///
    /// partition.insert("1", "abc")?;
    /// assert_eq!(partition.approximate_len(), 1);
    ///
    /// partition.remove("1")?;
    /// // Oops! approximate_len will not be reliable here
    /// assert_eq!(partition.approximate_len(), 2);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    #[must_use]
    pub fn approximate_len(&self) -> u64 {
        self.tree.approximate_len()
    }

    /// Scans the entire partition, returning the amount of items.
    ///
    /// ###### Caution
    ///
    /// This operation scans the entire partition: O(n) complexity!
    ///
    /// Never, under any circumstances, use .`len()` == 0 to check
    /// if the partition is empty, use [`PartitionHandle::is_empty`] instead.
    ///
    /// If you want an estimate, use [`PartitionHandle::approximate_len`] instead.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open()?;
    /// # let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
    /// assert_eq!(partition.len()?, 0);
    ///
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

        for kv in self.iter() {
            let _ = kv?;
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
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open()?;
    /// # let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
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
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open()?;
    /// # let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
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
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open()?;
    /// # let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
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
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open()?;
    /// # let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
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
    pub fn first_key_value(&self) -> crate::Result<Option<KvPair>> {
        Ok(self.tree.first_key_value()?)
    }

    /// Returns the last key-value pair in the partition.
    /// The key in this pair is the maximum key in the partition.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open()?;
    /// # let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
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
    pub fn last_key_value(&self) -> crate::Result<Option<KvPair>> {
        Ok(self.tree.last_key_value()?)
    }

    // NOTE: Used in tests
    #[doc(hidden)]
    pub fn rotate_memtable_and_wait(&self) -> crate::Result<()> {
        if self.rotate_memtable()? {
            while !self
                .flush_manager
                .read()
                .expect("lock is poisoned")
                .is_empty()
            {
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
        }
        Ok(())
    }

    /// Returns `true` if the memtable was indeed rotated.
    #[doc(hidden)]
    pub fn rotate_memtable(&self) -> crate::Result<bool> {
        log::debug!("Rotating memtable {:?}", self.name);

        log::trace!("partition: acquiring full write lock");
        let mut journal = self.journal.full_lock();

        // Rotate memtable
        let Some((yanked_id, yanked_memtable)) = self.tree.rotate_memtable() else {
            log::debug!("Got no sealed memtable, someone beat us to it");
            return Ok(false);
        };

        log::trace!("partition: acquiring journal manager lock");
        let mut journal_manager = self.journal_manager.write().expect("lock is poisoned");

        let seqno_map = {
            let partitions = self.partitions.write().expect("lock is poisoned");

            let mut map = HashMap::with_hasher(xxhash_rust::xxh3::Xxh3Builder::new());

            for (name, partition) in &*partitions {
                if let Some(lsn) = partition.tree.get_highest_memtable_seqno() {
                    map.insert(
                        name.clone(),
                        PartitionSeqNo {
                            lsn,
                            partition: partition.clone(),
                        },
                    );
                }
            }

            map.insert(
                self.name.clone(),
                PartitionSeqNo {
                    partition: self.clone(),
                    lsn: yanked_memtable
                        .get_highest_seqno()
                        .expect("sealed memtable is never empty"),
                },
            );

            map
        };

        journal_manager.rotate_journal(&mut journal, seqno_map)?;

        log::trace!("partition: acquiring flush manager lock");
        let mut flush_manager = self.flush_manager.write().expect("lock is poisoned");

        flush_manager.enqueue_task(
            self.name.clone(),
            FlushTask {
                id: yanked_id,
                partition: self.clone(),
                sealed_memtable: yanked_memtable,
            },
        );

        drop(journal_manager);
        drop(flush_manager);
        drop(journal);

        // Notify flush worker that new work has arrived
        self.flush_semaphore.release();

        Ok(true)
    }

    fn check_journal_size(&self) {
        loop {
            let bytes = self
                .journal_manager
                .read()
                .expect("lock is poisoned")
                .disk_space_used();

            if bytes <= self.keyspace_config.max_journaling_size_in_bytes {
                if bytes as f64 > self.keyspace_config.max_journaling_size_in_bytes as f64 * 0.9 {
                    log::info!(
                        "partition: write stall because 90% journal threshold has been reached"
                    );
                    std::thread::sleep(std::time::Duration::from_millis(500));
                }

                break;
            }

            log::debug!("partition: write halt because of too many journals");
            std::thread::sleep(std::time::Duration::from_millis(100)); // TODO: maybe exponential backoff
        }
    }

    fn check_write_stall(&self) {
        let seg_count = self.tree.first_level_segment_count();

        if seg_count > 10 {
            let sleep_us = get_write_delay(seg_count);
            log::info!("Stalling writes by {sleep_us}µs, many segments in L0...");
            self.compaction_manager.notify(self.clone());
            std::thread::sleep(Duration::from_micros(sleep_us));
        }
    }

    fn check_write_halt(&self) {
        while self.tree.first_level_segment_count() > 20 {
            log::info!("Halting writes until L0 is cleared up...");
            self.compaction_manager.notify(self.clone());
            std::thread::sleep(Duration::from_millis(1_000));
        }
    }

    pub(crate) fn check_memtable_overflow(&self, size: u32) -> crate::Result<()> {
        use std::sync::atomic::Ordering::Acquire;

        if size > self.max_memtable_size.load(Acquire) {
            self.rotate_memtable()?;
            self.check_journal_size();
            self.check_write_halt();
        }

        self.check_write_stall();

        Ok(())
    }

    pub(crate) fn check_write_buffer_size(&self, initial_size: u64) {
        if initial_size > self.keyspace_config.max_write_buffer_size_in_bytes {
            loop {
                let bytes = self.write_buffer_manager.get();

                if bytes < self.keyspace_config.max_write_buffer_size_in_bytes {
                    if bytes as f64
                        > self.keyspace_config.max_write_buffer_size_in_bytes as f64 * 0.9
                    {
                        log::info!(
                            "partition: write stall because 90% write buffer threshold has been reached"
                        );
                        std::thread::sleep(std::time::Duration::from_millis(500));
                    }
                    break;
                }

                log::info!("partition: write halt because of write buffer saturation");
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
        }
    }

    #[doc(hidden)]
    #[must_use]
    pub fn segment_count(&self) -> usize {
        self.tree.segment_count()
    }

    /// Opens a snapshot of this partition.
    #[must_use]
    pub fn snapshot(&self) -> crate::Snapshot {
        self.snapshot_at(self.seqno.get())
    }

    /// Opens a snapshot of this partition with a given sequence number.
    #[must_use]
    pub fn snapshot_at(&self, seqno: crate::Instant) -> crate::Snapshot {
        crate::Snapshot::new(
            self.tree.snapshot(seqno),
            SnapshotNonce::new(seqno, self.snapshot_tracker.clone()),
        )
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
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open()?;
    /// # let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
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
        if self.is_deleted.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(crate::Error::PartitionDeleted);
        }

        if self.is_poisoned.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(crate::Error::Poisoned);
        }

        let mut shard = self.journal.get_writer();

        let seqno = self.seqno.next();

        shard.writer.write(
            &BatchItem {
                key: key.as_ref().into(),
                value: value.as_ref().into(),
                partition: self.name.clone(),
                value_type: lsm_tree::ValueType::Value,
            },
            seqno,
        )?;
        drop(shard);

        let (item_size, memtable_size) = self.tree.insert(key, value, seqno);

        let write_buffer_size = self.write_buffer_manager.allocate(u64::from(item_size));

        self.check_memtable_overflow(memtable_size)?;
        self.check_write_buffer_size(write_buffer_size);

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
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open()?;
    /// # let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
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
        if self.is_deleted.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(crate::Error::PartitionDeleted);
        }

        if self.is_poisoned.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(crate::Error::Poisoned);
        }

        let mut shard = self.journal.get_writer();

        let seqno = self.seqno.next();

        shard.writer.write(
            &BatchItem {
                key: key.as_ref().into(),
                value: [].into(),
                partition: self.name.clone(),
                value_type: lsm_tree::ValueType::Tombstone,
            },
            seqno,
        )?;
        drop(shard);

        let (item_size, memtable_size) = self.tree.remove(key, seqno);

        let write_buffer_size = self.write_buffer_manager.allocate(u64::from(item_size));

        self.check_memtable_overflow(memtable_size)?;
        self.check_write_buffer_size(write_buffer_size);

        Ok(())
    }
}
