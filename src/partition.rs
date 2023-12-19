use crate::{
    batch::BatchItem,
    file::{LEVELS_MANIFEST_FILE, PARTITIONS_FOLDER, SEGMENTS_FOLDER},
    journal::{shard::JournalShard, Journal},
    levels::Levels,
    memtable::MemTable,
    prefix::Prefix,
    range::{MemTableGuard, Range},
    recovery::recover_segments,
    tree::{CompareAndSwapError, CompareAndSwapResult},
    value::{SeqNo, UserData, UserKey, ValueType},
    Config, Value,
};
use std::{
    collections::BTreeMap,
    ops::RangeBounds,
    path::Path,
    sync::{atomic::AtomicU64, Arc, RwLock, RwLockWriteGuard},
};

fn ignore_tombstone_value(item: Value) -> Option<Value> {
    if item.is_tombstone() {
        None
    } else {
        Some(item)
    }
}

// TODO: 0.3.0 partition needs separate config (TreeConfig vs PartitionConfig)
// TODO: need better Partition API
// TODO: something like Partition::create(&tree, ...)
// TODO: and Tree::partition should just panic if the partition doesn't exist
// TODO: OR return a handle from ::create and use that in Tree::partition

// TODO: also needed: PartitionHandle::drop -> consumes handle

// TODO: if the column family was deleted, the journal should not flush parts to the partition at all.
// TODO: so recover partitions before handling orphaned journals

#[allow(clippy::module_name_repetitions)]
pub struct PartitionInner {
    /// Partition name
    ///
    /// Defaults to "default"
    pub(crate) name: Arc<str>,

    /// Active memtable that is being written to
    pub(crate) active_memtable: Arc<RwLock<MemTable>>,

    /// Frozen memtables that are being flushed
    pub(crate) immutable_memtables: Arc<RwLock<BTreeMap<Arc<str>, Arc<MemTable>>>>,

    /// Levels manifest
    pub(crate) levels: Arc<RwLock<Levels>>,

    /// Journal of parent tree
    pub(crate) journal: Arc<Journal>,

    /// LSN
    pub(crate) next_lsn: Arc<AtomicU64>,

    /// Tree configuration
    pub(crate) config: Config,
}

#[doc(alias = "keyspace")]
#[doc(alias = "table")]
#[doc(alias = "locality group")]
#[doc(alias = "column family")]
#[derive(Clone)]
/// A partition is a logical keyspace inside the LSM-tree.
///
/// All operations on `Tree` operate on the "default" partition (literally called "default").
/// Use [`crate::Tree::partition`] to create and access a new partition.
/// Items inside a partition are stored physically separate from other partitions.
/// Each partition could be seen as a logical LSM-tree, with [`crate::Tree`] being a super structure
/// that houses the partitions.
///
/// Partitions are equivalent to `RocksDB`'s column families
/// and similar to `Bigtable`'s locality groups.
///
/// To access a partition inside a snapshot, use [`crate::Snapshot::partition`].
pub struct Partition(pub(crate) Arc<PartitionInner>);

impl std::ops::Deref for Partition {
    type Target = PartitionInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Partition {
    pub(crate) fn recover<P: AsRef<Path>>(
        path: P,
        name: Arc<str>,
        config: Config,
        memtable: MemTable,
        journal: Arc<Journal>,
        next_lsn: Arc<AtomicU64>,
    ) -> crate::Result<Self> {
        let partition_folder = path.as_ref();

        log::info!(
            "Recovering partition {name:?} from {}",
            partition_folder.display()
        );

        let segments = recover_segments(partition_folder, &config.block_cache)?;
        log::debug!(
            "Recovered {} segments for partition {name:?}",
            segments.len()
        );

        let mut levels = Levels::recover(partition_folder.join(LEVELS_MANIFEST_FILE), segments)?;
        levels.sort_levels();

        let inner = PartitionInner {
            name,
            active_memtable: Arc::new(RwLock::new(memtable)),
            config,
            immutable_memtables: Arc::default(),
            journal,
            levels: Arc::new(RwLock::new(levels)),
            next_lsn,
        };

        Ok(Self(Arc::new(inner)))
    }

    pub(crate) fn create_new(
        name: Arc<str>,
        journal: Arc<Journal>,
        next_lsn: Arc<AtomicU64>,
        config: Config,
    ) -> crate::Result<Self> {
        let partition_folder = config.path.join(PARTITIONS_FOLDER).join(&*name);
        log::info!(
            "Creating partition {name:?} at {}",
            partition_folder.display()
        );

        std::fs::create_dir_all(&partition_folder)?;
        let levels = Levels::create_new(
            config.level_count,
            partition_folder.join(LEVELS_MANIFEST_FILE),
        )?;

        std::fs::create_dir_all(partition_folder.join(SEGMENTS_FOLDER))?;

        #[cfg(not(target_os = "windows"))]
        {
            // fsync folders on Unix

            let folder = std::fs::File::open(partition_folder.join(SEGMENTS_FOLDER))?;
            folder.sync_all()?;

            let folder = std::fs::File::open(&partition_folder)?;
            folder.sync_all()?;
        }

        let inner = PartitionInner {
            name,
            active_memtable: Arc::new(RwLock::new(MemTable::default())),
            immutable_memtables: Arc::new(RwLock::new(BTreeMap::default())),
            levels: Arc::new(RwLock::new(levels)),
            journal,
            next_lsn,
            config,
        };

        Ok(Self(Arc::new(inner)))
    }

    /// Returns `true` if there are some segments that are being compacted.
    #[doc(hidden)]
    #[must_use]
    pub fn is_compacting(&self) -> bool {
        let levels = self.levels.read().expect("lock is poisoned");
        levels.is_compacting()
    }

    /// Counts the amount of segments currently in the partition.
    #[doc(hidden)]
    #[must_use]
    pub(crate) fn first_level_segment_count(&self) -> usize {
        self.levels
            .read()
            .expect("lock is poisoned")
            .first_level_segment_count()
    }

    /// Counts the amount of disk segments currently in the partition.
    /// #[doc(hidden)]
    #[must_use]
    pub fn segment_count(&self) -> usize {
        self.levels.read().expect("lock is poisoned").len()
    }

    /// Counts the amount of segments currently in the partition.
    #[must_use]
    pub fn disk_space(&self) -> u64 {
        self.levels
            .read()
            .expect("lock is poisoned")
            .get_all_segments()
            .values()
            .map(|x| x.metadata.file_size)
            .sum::<u64>()
    }

    fn append_entry(
        &self,
        mut shard: RwLockWriteGuard<'_, JournalShard>,
        value: Value,
    ) -> crate::Result<()> {
        let bytes_written_to_disk = shard.write(
            &BatchItem {
                partition: self.name.clone(),
                key: value.key.clone(),
                value: value.value.clone(),
                value_type: value.value_type,
            },
            value.seqno,
        )?;
        drop(shard);

        let memtable_lock = self.active_memtable.read().expect("lock is poisoned");
        memtable_lock.insert(value);

        // NOTE: Add some pointers to better approximate memory usage of memtable
        // Because the data is stored with less overhead than in memory
        let size = bytes_written_to_disk
            + std::mem::size_of::<UserKey>()
            + std::mem::size_of::<UserData>();

        memtable_lock
            .approximate_size
            .fetch_add(size as u32, std::sync::atomic::Ordering::Relaxed);

        drop(memtable_lock);

        // TODO: 0.3.0 flush

        /* if memtable_size > self.tree.config.max_memtable_size {
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
            crate::flush::start(&self.tree, self)?;
        } */

        Ok(())
    }

    pub(crate) fn get_lsn(&self) -> SeqNo {
        let segments = self
            .levels
            .read()
            .expect("lock is poisoned")
            .get_all_segments_flattened();

        segments
            .iter()
            .map(|x| x.metadata.seqnos.1 + 1)
            .max()
            .unwrap_or(0)
    }

    pub(crate) fn increment_lsn(&self) -> SeqNo {
        self.next_lsn
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel)
    }

    /// Inserts a key-value pair into the partition.
    ///
    /// If the key already exists, the item will be overwritten.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn insert<K: AsRef<[u8]>, V: AsRef<[u8]>>(&self, key: K, value: V) -> crate::Result<()> {
        let shard = self.journal.lock_shard();

        let value = Value::new(
            key.as_ref(),
            value.as_ref(),
            self.increment_lsn(),
            ValueType::Value,
        );

        self.append_entry(shard, value)?;

        Ok(())
    }

    /// Deletes an item from the partition.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn remove<K: AsRef<[u8]>>(&self, key: K) -> crate::Result<()> {
        let shard = self.journal.lock_shard();

        let value = Value::new(
            key.as_ref(),
            vec![],
            self.increment_lsn(),
            ValueType::Tombstone,
        );

        self.append_entry(shard, value)?;

        Ok(())
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

    /// Removes the item and returns its value if it was previously in the partition.
    ///
    /// This is less efficient than just deleting because it needs to do a read before deleting.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn remove_entry<K: AsRef<[u8]>>(&self, key: K) -> crate::Result<Option<UserData>> {
        self.fetch_update(key, |_| None::<UserData>)
    }

    /// Returns the first key-value pair in the partition.
    ///
    /// The key in this pair is the minimum key in the partition.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn first_key_value(&self) -> crate::Result<Option<(UserKey, UserData)>> {
        self.iter().into_iter().next().transpose()
    }

    /// Returns the last key-value pair in the partition.
    ///
    /// The key in this pair is the maximum key in the partition.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn last_key_value(&self) -> crate::Result<Option<(UserKey, UserData)>> {
        self.iter().into_iter().next_back().transpose()
    }

    /// Returns `true` if the partition is empty.
    ///
    /// This operation has O(1) complexity.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn is_empty(&self) -> crate::Result<bool> {
        self.first_key_value().map(|x| x.is_none())
    }

    /// Scans the entire partition, returning the amount of items.
    ///
    /// ###### Caution
    ///
    /// This operation scans the entire partition: O(n) complexity!
    ///
    /// Never, under any circumstances, use .len() == 0 to check
    /// if the partition is empty, use [`Partition::is_empty`] instead.
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

    /// Returns an iterator that scans through the entire partition.
    ///
    /// Avoid using this function, or limit it as otherwise it may scan a lot of items.
    pub fn iter(&self) -> Range {
        self.create_iter(None)
    }

    /// Returns an iterator over a range of items.
    ///
    /// Avoid using full or unbounded ranges as they may scan a lot of items (unless limited).
    pub fn range<K: AsRef<[u8]>, R: RangeBounds<K>>(&self, range: R) -> Range {
        self.create_range(range, None)
    }

    /// Returns an iterator over a prefixed set of items.
    ///
    /// Avoid using an empty prefix as it may scan a lot of items (unless limited).
    pub fn prefix<K: AsRef<[u8]>>(&self, prefix: K) -> Prefix {
        self.create_prefix(prefix.as_ref(), None)
    }

    pub(crate) fn create_iter(&self, seqno: Option<SeqNo>) -> Range {
        self.create_range::<UserKey, _>(.., seqno)
    }

    pub(crate) fn create_range<K: AsRef<[u8]>, R: RangeBounds<K>>(
        &self,
        range: R,
        seqno: Option<SeqNo>,
    ) -> Range {
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
                active: guardian::ArcRwLockReadGuardian::take(self.active_memtable.clone())
                    .expect("lock is poisoned"),
                immutable: guardian::ArcRwLockReadGuardian::take(self.immutable_memtables.clone())
                    .expect("lock is poisoned"),
            },
            bounds,
            segment_info,
            seqno,
        )
    }

    pub(crate) fn create_prefix<K: Into<UserKey>>(
        &self,
        prefix: K,
        seqno: Option<SeqNo>,
    ) -> Prefix {
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
                active: guardian::ArcRwLockReadGuardian::take(self.active_memtable.clone())
                    .expect("lock is poisoned"),
                immutable: guardian::ArcRwLockReadGuardian::take(self.immutable_memtables.clone())
                    .expect("lock is poisoned"),
            },
            prefix,
            segment_info,
            seqno,
        )
    }

    /// Retrieves an item from the partition.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> crate::Result<Option<UserData>> {
        Ok(self.get_internal_entry(key, true, None)?.map(|x| x.value))
    }

    #[doc(hidden)]
    pub fn get_internal_entry<K: AsRef<[u8]>>(
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

    /// Approximates the item count of the partition.
    ///
    /// This metric is only reliable for insert-only (no updates, deletes) workloads.
    /// Otherwise, the value may become less accurate over time
    /// and only converge to the real value time as compaction kicks in.
    ///
    /// This operation has O(1) complexity and can be used
    /// without feeling bad about it.
    pub fn approximate_len(&self) -> usize {
        let segment_size = self.levels.read().expect("lock is poisoned").len();

        let active_memtable_size = self
            .active_memtable
            .read()
            .expect("lock is poisoned")
            .items
            .len();

        let immutable_memtables_sizes = self
            .immutable_memtables
            .read()
            .iter()
            .map(|x| x.len())
            .sum::<usize>();

        segment_size + active_memtable_size + immutable_memtables_sizes
    }

    /// Returns `true` if the tree contains the specified key.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn contains_key<K: AsRef<[u8]>>(&self, key: K) -> crate::Result<bool> {
        self.get(key).map(|x| x.is_some())
    }
}
