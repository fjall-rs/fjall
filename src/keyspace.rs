// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    background_worker::{Activity, BackgroundWorker},
    batch::{Batch, PartitionKey},
    compaction::manager::CompactionManager,
    config::Config,
    file::{
        fsync_directory, FJALL_MARKER, JOURNALS_FOLDER, PARTITIONS_FOLDER, PARTITION_DELETED_MARKER,
    },
    flush::manager::FlushManager,
    journal::{manager::JournalManager, writer::PersistMode, Journal},
    monitor::Monitor,
    partition::name::is_valid_partition_name,
    poison_dart::PoisonDart,
    recovery::{recover_partitions, recover_sealed_memtables},
    snapshot_tracker::SnapshotTracker,
    stats::Stats,
    version::Version,
    write_buffer_manager::WriteBufferManager,
    HashMap, PartitionCreateOptions, PartitionHandle,
};
use lsm_tree::{AbstractTree, SequenceNumberCounter};
use std::{
    fs::{remove_dir_all, File},
    path::Path,
    sync::{
        atomic::{AtomicBool, AtomicUsize},
        Arc, RwLock,
    },
    time::Duration,
};
use std_semaphore::Semaphore;

pub type Partitions = HashMap<PartitionKey, PartitionHandle>;

#[allow(clippy::module_name_repetitions)]
pub struct KeyspaceInner {
    /// Dictionary of all partitions
    #[doc(hidden)]
    pub partitions: Arc<RwLock<Partitions>>,

    /// Journal (write-ahead-log/WAL)
    pub(crate) journal: Arc<Journal>,

    /// Keyspace configuration
    #[doc(hidden)]
    pub config: Config,

    /// Current sequence number
    pub(crate) seqno: SequenceNumberCounter,

    /// Current visible sequence number
    pub(crate) visible_seqno: SequenceNumberCounter,

    /// Caps write buffer size by flushing
    /// memtables to disk segments
    pub(crate) flush_manager: Arc<RwLock<FlushManager>>,

    /// Checks on-disk journal size and flushes memtables
    /// if needed, to garbage collect sealed journals
    pub(crate) journal_manager: Arc<RwLock<JournalManager>>,

    /// Notifies flush threads
    pub(crate) flush_semaphore: Arc<Semaphore>,

    /// Keeps track of which partitions are most likely to be
    /// candidates for compaction
    pub(crate) compaction_manager: CompactionManager,

    /// Stop signal when keyspace is dropped to stop background threads
    pub(crate) stop_signal: lsm_tree::stop_signal::StopSignal,

    /// Counter of background threads
    pub(crate) active_background_threads: Arc<AtomicUsize>,

    /// Keeps track of write buffer size
    pub(crate) write_buffer_manager: WriteBufferManager,

    /// True if fsync failed
    pub(crate) is_poisoned: Arc<AtomicBool>,

    pub(crate) stats: Arc<Stats>,

    #[doc(hidden)]
    pub snapshot_tracker: SnapshotTracker,
}

impl Drop for KeyspaceInner {
    fn drop(&mut self) {
        log::trace!("Dropping Keyspace");

        self.stop_signal.send();

        while self
            .active_background_threads
            .load(std::sync::atomic::Ordering::Relaxed)
            > 0
        {
            std::thread::sleep(std::time::Duration::from_micros(100));

            // NOTE: Trick threads into waking up
            self.flush_semaphore.release();
            self.compaction_manager.notify_empty();
        }

        // IMPORTANT: Break cyclic Arcs
        self.flush_manager
            .write()
            .expect("lock is poisoned")
            .clear();
        self.compaction_manager.clear();
        self.partitions.write().expect("lock is poisoned").clear();
        self.journal_manager
            .write()
            .expect("lock is poisoned")
            .clear();

        self.config.descriptor_table.clear();

        if self.config.clean_path_on_drop {
            log::info!(
                "Deleting keyspace because temporary=true: {:?}",
                self.config.path,
            );

            if let Err(err) = remove_dir_all(&self.config.path) {
                eprintln!("Failed to clean up path: {:?} - {err}", self.config.path);
            }
        }

        #[cfg(feature = "__internal_whitebox")]
        crate::drop::decrement_drop_counter();
    }
}

/// A keyspace is a single logical database
/// which can house multiple partitions
///
/// In your application, you should create a single keyspace
/// and keep it around for as long as needed
/// (as long as you are using its partitions).
#[derive(Clone)]
#[doc(alias = "database")]
#[doc(alias = "collection")]
pub struct Keyspace(pub(crate) Arc<KeyspaceInner>);

impl std::ops::Deref for Keyspace {
    type Target = KeyspaceInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Keyspace {
    /// Initializes a new atomic write batch.
    ///
    /// Items may be written to multiple partitions, which
    /// will be be updated atomically when the batch is committed.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open()?;
    /// # let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
    /// let mut batch = keyspace.batch();
    ///
    /// assert_eq!(partition.len()?, 0);
    /// batch.insert(&partition, "1", "abc");
    /// batch.insert(&partition, "3", "abc");
    /// batch.insert(&partition, "5", "abc");
    ///
    /// assert_eq!(partition.len()?, 0);
    ///
    /// batch.commit()?;
    /// assert_eq!(partition.len()?, 3);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    #[must_use]
    pub fn batch(&self) -> Batch {
        let mut batch = Batch::new(self.clone());

        if !self.config.manual_journal_persist {
            batch = batch.durability(Some(PersistMode::Buffer));
        }

        batch
    }

    /// Returns the current write buffer size (active + sealed memtables).
    #[must_use]
    pub fn write_buffer_size(&self) -> u64 {
        self.write_buffer_manager.get()
    }

    /// Returns the amount of completed memtable flushes.
    #[doc(hidden)]
    #[must_use]
    pub fn flushes_completed(&self) -> usize {
        self.stats
            .flushes_completed
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Returns the time all compactions took until now.
    #[doc(hidden)]
    #[must_use]
    pub fn time_compacting(&self) -> std::time::Duration {
        let us = self
            .stats
            .time_compacting
            .load(std::sync::atomic::Ordering::Relaxed);

        std::time::Duration::from_micros(us)
    }

    /// Returns the number of active compactions currently running.
    #[doc(hidden)]
    #[must_use]
    pub fn active_compactions(&self) -> usize {
        self.stats
            .active_compaction_count
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Returns the amount of completed compactions.
    #[doc(hidden)]
    #[must_use]
    pub fn compactions_completed(&self) -> usize {
        self.stats
            .compactions_completed
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Returns the amount of journals on disk.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open()?;
    /// # let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
    /// assert_eq!(1, keyspace.journal_count());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    #[must_use]
    pub fn journal_count(&self) -> usize {
        self.journal_manager
            .read()
            .expect("lock is poisoned")
            .journal_count()
    }

    /// Returns the disk space usage of the journal.
    #[doc(hidden)]
    #[must_use]
    pub fn journal_disk_space(&self) -> u64 {
        // TODO: 3.0.0 error is not handled because that would break the API...
        self.journal.get_writer().len().unwrap_or_default()
            + self
                .journal_manager
                .read()
                .expect("lock is poisoned")
                .disk_space_used()
    }

    /// Returns the disk space usage of the entire keyspace.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open()?;
    /// # let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
    /// assert!(keyspace.disk_space() > 0);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    pub fn disk_space(&self) -> u64 {
        let partitions_size = self
            .partitions
            .read()
            .expect("lock is poisoned")
            .values()
            .map(PartitionHandle::disk_space)
            .sum::<u64>();

        self.journal_disk_space() + partitions_size
    }

    /// Flushes the active journal. The durability depends on the [`PersistMode`]
    /// used.
    ///
    /// Persisting only affects durability, NOT consistency! Even without flushing
    /// data is crash-safe.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, PersistMode, Keyspace, PartitionCreateOptions};
    /// # let folder = tempfile::tempdir()?;
    /// let keyspace = Config::new(folder).open()?;
    /// let items = keyspace.open_partition("my_items", PartitionCreateOptions::default())?;
    ///
    /// items.insert("a", "hello")?;
    ///
    /// keyspace.persist(PersistMode::SyncAll)?;
    /// #
    /// # Ok::<_, fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Returns error, if an IO error occurred.
    pub fn persist(&self, mode: PersistMode) -> crate::Result<()> {
        if self.is_poisoned.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(crate::Error::Poisoned);
        }

        if let Err(e) = self.journal.persist(mode) {
            self.is_poisoned
                .store(true, std::sync::atomic::Ordering::Release);

            log::error!(
                "flush failed, which is a FATAL, and possibly hardware-related, failure: {e:?}"
            );

            return Err(crate::Error::Poisoned);
        }

        Ok(())
    }

    #[doc(hidden)]
    #[must_use]
    pub fn cache_capacity(&self) -> u64 {
        self.config.cache.capacity()
    }

    /// Opens a keyspace in the given directory.
    ///
    /// # Errors
    ///
    /// Returns error, if an IO error occurred.
    pub fn open(config: Config) -> crate::Result<Self> {
        log::debug!(
            "cache capacity={}MiB",
            config.cache.capacity() / 1_024 / 1_024,
        );

        let keyspace = Self::create_or_recover(config)?;
        keyspace.start_background_threads()?;

        #[cfg(feature = "__internal_whitebox")]
        crate::drop::increment_drop_counter();

        Ok(keyspace)
    }

    /// Same as [`Keyspace::open`], but does not start background threads.
    ///
    /// Needed to open a keyspace without threads for testing.
    ///
    /// Should not be user-facing.
    #[doc(hidden)]
    pub fn create_or_recover(config: Config) -> crate::Result<Self> {
        log::info!("Opening keyspace at {:?}", config.path);

        if config.path.join(FJALL_MARKER).try_exists()? {
            Self::recover(config)
        } else {
            Self::create_new(config)
        }
    }

    /// Starts background threads that maintain the keyspace.
    ///
    /// Should not be called, unless in [`Keyspace::open`]
    /// and should definitely not be user-facing.
    pub(crate) fn start_background_threads(&self) -> crate::Result<()> {
        if self.config.flush_workers_count > 0 {
            self.spawn_flush_worker()?;

            for _ in 0..self
                .flush_manager
                .read()
                .expect("lock is poisoned")
                .queue_count()
            {
                self.flush_semaphore.release();
            }
        }

        log::debug!(
            "Spawning {} compaction threads",
            self.config.compaction_workers_count
        );

        for _ in 0..self.config.compaction_workers_count {
            self.spawn_compaction_worker()?;
        }

        if let Some(ms) = self.config.fsync_ms {
            self.spawn_fsync_thread(ms.into())?;
        }

        self.spawn_monitor_thread()
    }

    /// Destroys the partition, removing all data associated with it.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn delete_partition(&self, handle: PartitionHandle) -> crate::Result<()> {
        let partition_path = handle.path();

        let file = File::create(partition_path.join(PARTITION_DELETED_MARKER))?;
        file.sync_all()?;

        // IMPORTANT: fsync folder on Unix
        fsync_directory(partition_path)?;

        handle
            .is_deleted
            .store(true, std::sync::atomic::Ordering::Release);

        // IMPORTANT: Care, locks partitions map
        self.compaction_manager.remove_partition(&handle.name);

        self.flush_manager
            .write()
            .expect("lock is poisoned")
            .remove_partition(&handle.name);

        self.partitions
            .write()
            .expect("lock is poisoned")
            .remove(&handle.name);

        Ok(())
    }

    /// Creates or opens a keyspace partition.
    ///
    /// If the partition does not yet exist, it will be created configured with `create_options`.
    /// Otherwise simply a handle to the existing partition will be returned.
    ///
    /// Partition names can be up to 255 characters long, can not be empty and
    /// can only contain alphanumerics, underscore (`_`), dash (`-`), hash tag (`#`) and dollar (`$`).
    ///
    /// # Errors
    ///
    /// Returns error, if an IO error occurred.
    ///
    /// # Panics
    ///
    /// Panics if the partition name is invalid.
    pub fn open_partition(
        &self,
        name: &str,
        create_options: PartitionCreateOptions,
    ) -> crate::Result<PartitionHandle> {
        assert!(is_valid_partition_name(name));

        let mut partitions = self.partitions.write().expect("lock is poisoned");

        Ok(if let Some(partition) = partitions.get(name) {
            partition.clone()
        } else {
            let name: PartitionKey = name.into();

            let handle = PartitionHandle::create_new(self, name.clone(), create_options)?;
            partitions.insert(name, handle.clone());

            #[cfg(feature = "__internal_whitebox")]
            crate::drop::increment_drop_counter();

            handle
        })
    }

    /// Returns the amount of partitions.
    #[must_use]
    pub fn partition_count(&self) -> usize {
        self.partitions.read().expect("lock is poisoned").len()
    }

    /// Gets a list of all partition names in the keyspace.
    #[must_use]
    pub fn list_partitions(&self) -> Vec<PartitionKey> {
        self.partitions
            .read()
            .expect("lock is poisoned")
            .keys()
            .cloned()
            .collect()
    }

    /// Returns `true` if the partition with the given name exists.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open()?;
    /// assert!(!keyspace.partition_exists("default"));
    /// keyspace.open_partition("default", PartitionCreateOptions::default())?;
    /// assert!(keyspace.partition_exists("default"));
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    #[must_use]
    pub fn partition_exists(&self, name: &str) -> bool {
        self.partitions
            .read()
            .expect("lock is poisoned")
            .contains_key(name)
    }

    /// Gets the current sequence number.
    ///
    /// Can be used to start a cross-partition snapshot, using [`PartitionHandle::snapshot_at`].
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open()?;
    /// let partition1 = keyspace.open_partition("default", PartitionCreateOptions::default())?;
    /// let partition2 = keyspace.open_partition("another", PartitionCreateOptions::default())?;
    ///
    /// partition1.insert("abc1", "abc")?;
    /// partition2.insert("abc2", "abc")?;
    ///
    /// let instant = keyspace.instant();
    /// let snapshot1 = partition1.snapshot_at(instant);
    /// let snapshot2 = partition2.snapshot_at(instant);
    ///
    /// assert!(partition1.contains_key("abc1")?);
    /// assert!(partition2.contains_key("abc2")?);
    ///
    /// assert!(snapshot1.contains_key("abc1")?);
    /// assert!(snapshot2.contains_key("abc2")?);
    ///
    /// partition1.insert("def1", "def")?;
    /// partition2.insert("def2", "def")?;
    ///
    /// assert!(!snapshot1.contains_key("def1")?);
    /// assert!(!snapshot2.contains_key("def2")?);
    ///
    /// assert!(partition1.contains_key("def1")?);
    /// assert!(partition2.contains_key("def2")?);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    #[must_use]
    pub fn instant(&self) -> crate::Instant {
        self.visible_seqno.get()
    }

    fn check_version<P: AsRef<Path>>(path: P) -> crate::Result<()> {
        let bytes = std::fs::read(path.as_ref().join(FJALL_MARKER))?;

        if let Some(version) = Version::parse_file_header(&bytes) {
            if version != Version::V2 {
                return Err(crate::Error::InvalidVersion(Some(version)));
            }
        } else {
            return Err(crate::Error::InvalidVersion(None));
        }

        Ok(())
    }

    /// Recovers existing keyspace from directory.
    #[allow(clippy::too_many_lines)]
    #[doc(hidden)]
    pub fn recover(config: Config) -> crate::Result<Self> {
        log::info!("Recovering keyspace at {:?}", config.path);

        // TODO:
        // let recovery_mode = config.journal_recovery_mode;

        // Check version
        Self::check_version(&config.path)?;

        // Reload active journal
        let journals_folder = config.path.join(JOURNALS_FOLDER);
        let journal_recovery = Journal::recover(journals_folder)?;
        log::debug!("journal recovery result: {journal_recovery:#?}");

        let active_journal = Arc::new(journal_recovery.active);
        active_journal.get_writer().persist(PersistMode::SyncAll)?;

        let sealed_journals = journal_recovery.sealed;

        let journal_manager = JournalManager::from_active(active_journal.path());

        // Construct (empty) keyspace, then fill back with partition data
        let inner = KeyspaceInner {
            config,
            journal: active_journal,
            partitions: Arc::new(RwLock::new(Partitions::with_capacity_and_hasher(
                10,
                xxhash_rust::xxh3::Xxh3Builder::new(),
            ))),
            seqno: SequenceNumberCounter::default(),
            visible_seqno: SequenceNumberCounter::default(),
            flush_manager: Arc::new(RwLock::new(FlushManager::new())),
            journal_manager: Arc::new(RwLock::new(journal_manager)),
            flush_semaphore: Arc::new(Semaphore::new(0)),
            compaction_manager: CompactionManager::default(),
            stop_signal: lsm_tree::stop_signal::StopSignal::default(),
            active_background_threads: Arc::default(),
            write_buffer_manager: WriteBufferManager::default(),
            is_poisoned: Arc::default(),
            snapshot_tracker: SnapshotTracker::default(),
            stats: Arc::default(),
        };

        let keyspace = Self(Arc::new(inner));

        // Recover partitions
        recover_partitions(&keyspace)?;

        // Recover sealed memtables by walking through old journals
        recover_sealed_memtables(
            &keyspace,
            &sealed_journals
                .into_iter()
                .map(|(_, x)| x)
                .collect::<Vec<_>>(),
        )?;

        {
            let partitions = keyspace.partitions.read().expect("lock is poisoned");

            #[cfg(debug_assertions)]
            for partition in partitions.values() {
                // NOTE: If this triggers, the last sealed memtable
                // was not correctly rotated
                debug_assert!(
                    partition.tree.lock_active_memtable().is_empty(),
                    "active memtable is not empty - this is a bug"
                );
            }

            // NOTE: We only need to recover the active journal, if it actually existed before
            // nothing to recover, if we just created it
            if !journal_recovery.was_active_created {
                log::trace!("Recovering active memtables from active journal");

                let reader = keyspace.journal.get_reader()?;

                for batch in reader {
                    let batch = batch?;

                    for item in batch.items {
                        if let Some(partition) = partitions.get(&item.partition) {
                            let tree = &partition.tree;

                            match item.value_type {
                                lsm_tree::ValueType::Value => {
                                    tree.insert(item.key, item.value, batch.seqno);
                                }
                                lsm_tree::ValueType::Tombstone => {
                                    tree.remove(item.key, batch.seqno);
                                }
                                lsm_tree::ValueType::WeakTombstone => {
                                    tree.remove_weak(item.key, batch.seqno);
                                }
                            }
                        }
                    }
                }

                for partition in partitions.values() {
                    let size = partition.tree.active_memtable_size().into();

                    log::trace!(
                        "Recovered active memtable of size {size}B for partition {:?} ({} items)",
                        partition.name,
                        partition.tree.lock_active_memtable().len(),
                    );

                    // IMPORTANT: Add active memtable size to current write buffer size
                    keyspace.write_buffer_manager.allocate(size);

                    // Recover seqno
                    let maybe_next_seqno = partition
                        .tree
                        .get_highest_seqno()
                        .map(|x| x + 1)
                        .unwrap_or_default();

                    keyspace
                        .seqno
                        .fetch_max(maybe_next_seqno, std::sync::atomic::Ordering::AcqRel);

                    log::debug!("Keyspace seqno is now {}", keyspace.seqno.get());
                }
            }
        }

        keyspace.visible_seqno.store(
            keyspace.seqno.load(std::sync::atomic::Ordering::Acquire),
            std::sync::atomic::Ordering::Release,
        );

        log::trace!("Recovery successful");

        Ok(keyspace)
    }

    #[doc(hidden)]
    pub fn create_new(config: Config) -> crate::Result<Self> {
        let path = config.path.clone();
        log::debug!("Creating keyspace at {path:?}");

        std::fs::create_dir_all(&path)?;

        let marker_path = path.join(FJALL_MARKER);
        assert!(!marker_path.try_exists()?);

        let journal_folder_path = path.join(JOURNALS_FOLDER);
        let partition_folder_path = path.join(PARTITIONS_FOLDER);

        std::fs::create_dir_all(&journal_folder_path)?;
        std::fs::create_dir_all(&partition_folder_path)?;

        let active_journal_path = journal_folder_path.join("0");
        let journal = Journal::create_new(&active_journal_path)?;
        let journal = Arc::new(journal);

        let inner = KeyspaceInner {
            config,
            journal,
            partitions: Arc::new(RwLock::new(Partitions::with_capacity_and_hasher(
                10,
                xxhash_rust::xxh3::Xxh3Builder::new(),
            ))),
            seqno: SequenceNumberCounter::default(),
            visible_seqno: SequenceNumberCounter::default(),
            flush_manager: Arc::new(RwLock::new(FlushManager::new())),
            journal_manager: Arc::new(RwLock::new(JournalManager::from_active(
                active_journal_path,
            ))),
            flush_semaphore: Arc::new(Semaphore::new(0)),
            compaction_manager: CompactionManager::default(),
            stop_signal: lsm_tree::stop_signal::StopSignal::default(),
            active_background_threads: Arc::default(),
            write_buffer_manager: WriteBufferManager::default(),
            is_poisoned: Arc::default(),
            snapshot_tracker: SnapshotTracker::default(),
            stats: Arc::default(),
        };

        // NOTE: Lastly, fsync .fjall marker, which contains the version
        // -> the keyspace is fully initialized
        let mut file = std::fs::File::create(marker_path)?;
        Version::V2.write_file_header(&mut file)?;
        file.sync_all()?;

        // IMPORTANT: fsync folders on Unix
        fsync_directory(&journal_folder_path)?;
        fsync_directory(&partition_folder_path)?;
        fsync_directory(&path)?;

        Ok(Self(Arc::new(inner)))
    }

    fn spawn_monitor_thread(&self) -> crate::Result<()> {
        const NAME: &str = "monitor";

        let monitor = Monitor::new(self);

        let poison_dart = PoisonDart::new(NAME, self.is_poisoned.clone());
        let thread_counter = self.active_background_threads.clone();
        let stop_signal = self.stop_signal.clone();

        let worker = BackgroundWorker::new(monitor, poison_dart, thread_counter, stop_signal);

        std::thread::Builder::new()
            .name(NAME.into())
            .spawn(move || {
                worker.start();
            })
            .map(|_| ())
            .map_err(Into::into)
    }

    fn spawn_fsync_thread(&self, ms: u64) -> crate::Result<()> {
        const NAME: &str = "syncer";

        struct Syncer {
            journal: Arc<Journal>,
            sleep_dur: Duration,
        }

        impl Activity for Syncer {
            fn name(&self) -> &'static str {
                NAME
            }

            fn run(&mut self) -> crate::Result<()> {
                std::thread::sleep(self.sleep_dur);
                self.journal.persist(PersistMode::SyncAll)?;
                Ok(())
            }
        }

        let syncer = Syncer {
            journal: self.journal.clone(),
            sleep_dur: Duration::from_millis(ms),
        };

        let poison_dart = PoisonDart::new(NAME, self.is_poisoned.clone());
        let thread_counter = self.active_background_threads.clone();
        let stop_signal = self.stop_signal.clone();

        let worker = BackgroundWorker::new(syncer, poison_dart, thread_counter, stop_signal);

        std::thread::Builder::new()
            .name(NAME.into())
            .spawn(move || {
                worker.start();
            })
            .map(|_| ())
            .map_err(Into::into)
    }

    fn spawn_compaction_worker(&self) -> crate::Result<()> {
        const NAME: &str = "compactor";

        struct Compactor {
            manager: CompactionManager,
            snapshot_tracker: SnapshotTracker,
            stats: Arc<Stats>,
        }

        impl Activity for Compactor {
            fn name(&self) -> &'static str {
                NAME
            }

            fn run(&mut self) -> crate::Result<()> {
                log::trace!("{:?}: waiting for work", self.name());
                self.manager.wait_for();
                crate::compaction::worker::run(&self.manager, &self.snapshot_tracker, &self.stats)?;
                Ok(())
            }
        }

        let compactor = Compactor {
            manager: self.compaction_manager.clone(),
            snapshot_tracker: self.snapshot_tracker.clone(),
            stats: self.stats.clone(),
        };

        let poison_dart = PoisonDart::new(NAME, self.is_poisoned.clone());
        let thread_counter = self.active_background_threads.clone();
        let stop_signal = self.stop_signal.clone();

        let worker = BackgroundWorker::new(compactor, poison_dart, thread_counter, stop_signal);

        std::thread::Builder::new()
            .name(NAME.into())
            .spawn(move || {
                worker.start();
            })
            .map(|_| ())
            .map_err(Into::into)
    }

    /// Only used for internal testing.
    ///
    /// Should NOT be called when there is a flush worker active already!!!
    #[doc(hidden)]
    pub fn force_flush(&self) -> crate::Result<()> {
        let parallelism = self.config.flush_workers_count;

        crate::flush::worker::run(
            &self.flush_manager,
            &self.journal_manager,
            &self.compaction_manager,
            &self.write_buffer_manager,
            &self.snapshot_tracker,
            parallelism,
            &self.stats,
        )
    }

    fn spawn_flush_worker(&self) -> crate::Result<()> {
        const NAME: &str = "flusher";

        struct Flusher {
            parallelism: usize,
            flush_semaphore: Arc<Semaphore>,
            flush_manager: Arc<RwLock<FlushManager>>,
            journal_manager: Arc<RwLock<JournalManager>>,
            write_buffer_manager: WriteBufferManager,
            compaction_manager: CompactionManager,
            snapshot_tracker: SnapshotTracker,
            stats: Arc<Stats>,
        }

        impl Activity for Flusher {
            fn name(&self) -> &'static str {
                NAME
            }

            fn run(&mut self) -> crate::Result<()> {
                log::trace!("{:?}: waiting for work", self.name());

                self.flush_semaphore.acquire();

                crate::flush::worker::run(
                    &self.flush_manager,
                    &self.journal_manager,
                    &self.compaction_manager,
                    &self.write_buffer_manager,
                    &self.snapshot_tracker,
                    self.parallelism,
                    &self.stats,
                )?;

                Ok(())
            }
        }

        let flusher = Flusher {
            flush_manager: self.flush_manager.clone(),
            journal_manager: self.journal_manager.clone(),
            compaction_manager: self.compaction_manager.clone(),
            flush_semaphore: self.flush_semaphore.clone(),
            write_buffer_manager: self.write_buffer_manager.clone(),
            snapshot_tracker: self.snapshot_tracker.clone(),
            stats: self.stats.clone(),
            parallelism: self.config.flush_workers_count,
        };

        let poison_dart = PoisonDart::new(NAME, self.is_poisoned.clone());
        let thread_counter = self.active_background_threads.clone();
        let stop_signal = self.stop_signal.clone();

        let worker = BackgroundWorker::new(flusher, poison_dart, thread_counter, stop_signal);

        std::thread::Builder::new()
            .name(NAME.into())
            .spawn(move || {
                worker.start();
            })
            .map(|_| ())
            .map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_log::test;

    // TODO: 3.0.0 if we store the partition as a monotonic integer
    // and the partition's name inside the partition options/manifest
    // we could allow all UTF-8 characters for partition names
    //
    // https://github.com/fjall-rs/fjall/issues/89
    #[test]
    pub fn test_exotic_partition_names() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;
        let config = Config::new(&folder);
        let keyspace = Keyspace::create_or_recover(config)?;

        for name in ["hello$world", "hello#world", "hello.world", "hello_world"] {
            let db = keyspace.open_partition(name, Default::default())?;
            db.insert("a", "a")?;
            assert_eq!(1, db.len()?);
        }

        Ok(())
    }

    #[test]
    pub fn recover_after_rotation_multiple_partitions() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;

        {
            let config = Config::new(&folder);
            let keyspace = Keyspace::create_or_recover(config)?;
            let db = keyspace.open_partition("default", Default::default())?;
            let db2 = keyspace.open_partition("default2", Default::default())?;

            db.insert("a", "a")?;
            db2.insert("a", "a")?;
            assert_eq!(1, db.len()?);
            assert_eq!(1, db2.len()?);

            assert_eq!(None, db.tree.get_highest_persisted_seqno());
            assert_eq!(None, db2.tree.get_highest_persisted_seqno());

            db.rotate_memtable()?;

            assert_eq!(1, db.len()?);
            assert_eq!(1, db.tree.sealed_memtable_count());

            assert_eq!(1, db2.len()?);
            assert_eq!(0, db2.tree.sealed_memtable_count());

            db2.insert("b", "b")?;
            db2.rotate_memtable()?;

            assert_eq!(1, db.len()?);
            assert_eq!(1, db.tree.sealed_memtable_count());

            assert_eq!(2, db2.len()?);
            assert_eq!(1, db2.tree.sealed_memtable_count());
        }

        {
            // IMPORTANT: We need to allocate enough flush workers
            // because on CI there may not be enough cores by default
            // so the result would be wrong
            let config = Config::new(&folder).flush_workers(16);
            let keyspace = Keyspace::create_or_recover(config)?;
            let db = keyspace.open_partition("default", Default::default())?;
            let db2 = keyspace.open_partition("default2", Default::default())?;

            assert_eq!(1, db.len()?);
            assert_eq!(1, db.tree.sealed_memtable_count());

            assert_eq!(2, db2.len()?);
            assert_eq!(2, db2.tree.sealed_memtable_count());

            assert_eq!(3, keyspace.journal_count());

            keyspace.force_flush()?;

            assert_eq!(1, db.len()?);
            assert_eq!(0, db.tree.sealed_memtable_count());

            assert_eq!(2, db2.len()?);
            assert_eq!(0, db2.tree.sealed_memtable_count());

            assert_eq!(1, keyspace.journal_count());

            assert_eq!(Some(0), db.tree.get_highest_persisted_seqno());
            assert_eq!(Some(2), db2.tree.get_highest_persisted_seqno());
        }

        Ok(())
    }

    #[test]
    pub fn recover_after_rotation() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;

        {
            let config = Config::new(&folder);
            let keyspace = Keyspace::create_or_recover(config)?;
            let db = keyspace.open_partition("default", Default::default())?;

            db.insert("a", "a")?;
            assert_eq!(1, db.len()?);

            db.rotate_memtable()?;

            assert_eq!(1, db.len()?);
            assert_eq!(1, db.tree.sealed_memtable_count());
        }

        {
            let config = Config::new(&folder);
            let keyspace = Keyspace::create_or_recover(config)?;
            let db = keyspace.open_partition("default", Default::default())?;

            assert_eq!(1, db.len()?);
            assert_eq!(1, db.tree.sealed_memtable_count());
            assert_eq!(2, keyspace.journal_count());

            keyspace.force_flush()?;

            assert_eq!(1, db.len()?);
            assert_eq!(0, db.tree.sealed_memtable_count());
            assert_eq!(1, keyspace.journal_count());
        }

        Ok(())
    }

    #[test]
    pub fn force_flush_multiple_partitions() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;

        let config = Config::new(folder);
        let keyspace = Keyspace::create_or_recover(config)?;
        let db = keyspace.open_partition("default", Default::default())?;
        let db2 = keyspace.open_partition("default2", Default::default())?;

        assert_eq!(0, keyspace.write_buffer_size());

        assert_eq!(0, db.segment_count());
        assert_eq!(0, db2.segment_count());

        assert_eq!(
            0,
            keyspace
                .journal_manager
                .read()
                .expect("lock is poisoned")
                .sealed_journal_count()
        );

        assert_eq!(
            0,
            keyspace
                .flush_manager
                .read()
                .expect("lock is poisoned")
                .queued_size()
        );

        assert_eq!(
            0,
            keyspace
                .flush_manager
                .read()
                .expect("lock is poisoned")
                .len()
        );

        for _ in 0..100 {
            db.insert(nanoid::nanoid!(), "abc")?;
            db2.insert(nanoid::nanoid!(), "abc")?;
        }

        db.rotate_memtable()?;

        assert_eq!(
            1,
            keyspace
                .flush_manager
                .read()
                .expect("lock is poisoned")
                .len()
        );

        assert_eq!(
            1,
            keyspace
                .journal_manager
                .read()
                .expect("lock is poisoned")
                .sealed_journal_count()
        );

        for _ in 0..100 {
            db2.insert(nanoid::nanoid!(), "abc")?;
        }

        db2.rotate_memtable()?;

        assert_eq!(
            2,
            keyspace
                .flush_manager
                .read()
                .expect("lock is poisoned")
                .len()
        );

        assert_eq!(
            2,
            keyspace
                .journal_manager
                .read()
                .expect("lock is poisoned")
                .sealed_journal_count()
        );

        assert_eq!(0, db.segment_count());
        assert_eq!(0, db2.segment_count());

        keyspace.force_flush()?;

        assert_eq!(
            0,
            keyspace
                .flush_manager
                .read()
                .expect("lock is poisoned")
                .queued_size()
        );

        assert_eq!(
            0,
            keyspace
                .flush_manager
                .read()
                .expect("lock is poisoned")
                .len()
        );

        assert_eq!(
            0,
            keyspace
                .journal_manager
                .read()
                .expect("lock is poisoned")
                .sealed_journal_count()
        );

        assert_eq!(0, keyspace.write_buffer_size());
        assert_eq!(1, db.segment_count());
        assert_eq!(1, db2.segment_count());

        Ok(())
    }

    #[test]
    pub fn force_flush() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;

        let config = Config::new(folder);
        let keyspace = Keyspace::create_or_recover(config)?;
        let db = keyspace.open_partition("default", Default::default())?;

        assert_eq!(0, keyspace.write_buffer_size());

        assert_eq!(0, db.segment_count());

        assert_eq!(
            0,
            keyspace
                .journal_manager
                .read()
                .expect("lock is poisoned")
                .sealed_journal_count()
        );

        assert_eq!(
            0,
            keyspace
                .flush_manager
                .read()
                .expect("lock is poisoned")
                .queued_size()
        );

        assert_eq!(
            0,
            keyspace
                .flush_manager
                .read()
                .expect("lock is poisoned")
                .len()
        );

        for _ in 0..100 {
            db.insert(nanoid::nanoid!(), "abc")?;
        }

        db.rotate_memtable()?;

        assert_eq!(
            1,
            keyspace
                .flush_manager
                .read()
                .expect("lock is poisoned")
                .len()
        );

        assert_eq!(
            1,
            keyspace
                .journal_manager
                .read()
                .expect("lock is poisoned")
                .sealed_journal_count()
        );

        assert_eq!(0, db.segment_count());

        keyspace.force_flush()?;

        assert_eq!(
            0,
            keyspace
                .flush_manager
                .read()
                .expect("lock is poisoned")
                .queued_size()
        );

        assert_eq!(
            0,
            keyspace
                .flush_manager
                .read()
                .expect("lock is poisoned")
                .len()
        );

        assert_eq!(
            0,
            keyspace
                .journal_manager
                .read()
                .expect("lock is poisoned")
                .sealed_journal_count()
        );

        assert_eq!(0, keyspace.write_buffer_size());
        assert_eq!(1, db.segment_count());

        Ok(())
    }
}
