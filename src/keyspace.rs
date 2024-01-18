use crate::{
    batch::{Batch, PartitionKey},
    compaction::manager::CompactionManager,
    config::Config,
    file::{
        FJALL_MARKER, FLUSH_MARKER, JOURNALS_FOLDER, PARTITIONS_FOLDER, PARTITION_DELETED_MARKER,
    },
    flush::manager::FlushManager,
    journal::{manager::JournalManager, shard::RecoveryMode, Journal},
    monitor::Monitor,
    partition::name::is_valid_partition_name,
    recovery::{recover_partitions, recover_sealed_memtables},
    version::Version,
    write_buffer_manager::WriteBufferManager,
    PartitionCreateOptions, PartitionHandle,
};
use lsm_tree::{id::generate_segment_id, MemTable, SequenceNumberCounter};
use std::{
    collections::HashMap,
    fs::File,
    path::Path,
    sync::{atomic::AtomicUsize, Arc, RwLock},
};
use std_semaphore::Semaphore;

pub type Partitions = HashMap<PartitionKey, PartitionHandle>;

#[allow(clippy::module_name_repetitions)]
pub struct KeyspaceInner {
    pub(crate) partitions: Arc<RwLock<Partitions>>,
    pub(crate) journal: Arc<Journal>,
    pub(crate) config: Config,
    pub(crate) seqno: SequenceNumberCounter,
    pub(crate) flush_manager: Arc<RwLock<FlushManager>>,
    pub(crate) journal_manager: Arc<RwLock<JournalManager>>,
    pub(crate) flush_semaphore: Arc<Semaphore>,
    pub(crate) compaction_manager: CompactionManager,
    pub(crate) stop_signal: lsm_tree::stop_signal::StopSignal,
    pub(crate) active_background_threads: Arc<AtomicUsize>,
    pub(crate) write_buffer_manager: WriteBufferManager,
}

impl Drop for KeyspaceInner {
    fn drop(&mut self) {
        log::trace!("Dropping Keyspace, trying to flush journal");

        self.stop_signal.send();

        if let Err(e) = self.journal.flush(true) {
            log::error!("Flush error on drop: {e:?}");
        }

        while self
            .active_background_threads
            .load(std::sync::atomic::Ordering::Relaxed)
            > 0
        {
            std::thread::sleep(std::time::Duration::from_millis(10));

            // NOTE: Trick threads into waking up
            self.flush_semaphore.release();
            self.compaction_manager.notify_empty();
        }

        self.config.descriptor_table.clear();
    }
}

/// A keyspace is a single logical database
/// which houses multiple partitions
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
    /// will be be updated atomically if the batch is committed.
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
        Batch::new(self.clone())
    }

    /// Returns the current write buffer size (active + sealed memtables)
    #[must_use]
    pub fn write_buffer_size(&self) -> u64 {
        self.write_buffer_manager.get()
    }

    /// Returns the amount of journals on disk
    #[must_use]
    pub fn journal_count(&self) -> usize {
        self.journal_manager
            .read()
            .expect("lock is poisoned")
            .journal_count()
    }

    /// Returns the disk space usage of the entire keyspace
    pub fn disk_space(&self) -> crate::Result<u64> {
        let journal_size = fs_extra::dir::get_size(&self.journal.path)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("{:?}", e.kind)))?;

        let partitions_lock = self.partitions.read().expect("lock is poisoned");

        let partitions_size = partitions_lock
            .values()
            .map(PartitionHandle::disk_space)
            .sum::<u64>();

        Ok(journal_size + partitions_size)
    }

    /// Flushes the active journal using fsyncdata, making sure recently written data is durable
    ///
    /// This has a dramatic, negative performance impact on writes by 100-1000x.
    ///
    /// Persisting only affects durability, NOT consistency! Even without flushing
    /// data is (or should be) crash-safe.
    ///
    /// # Errors
    ///
    /// Returns error, if an IO error occured.
    pub fn persist(&self) -> crate::Result<()> {
        self.journal.flush(false)?;
        Ok(())
    }

    /// Opens a keyspace in the given directory.
    ///
    /// # Errors
    ///
    /// Returns error, if an IO error occured.
    pub fn open(config: Config) -> crate::Result<Self> {
        let compaction_workers_count = config.compaction_workers_count;
        let keyspace = Self::create_or_recover(config)?;
        keyspace.start_background_threads(compaction_workers_count);
        Ok(keyspace)
    }

    /// Same as [`Keyspace::open`], but does not start background threads.
    ///
    /// Needed to open a keyspace without threads for testing.
    ///
    /// Should not be user-facing.
    fn create_or_recover(config: Config) -> crate::Result<Self> {
        log::info!("Opening keyspace at {}", config.path.display());

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
    fn start_background_threads(&self, compaction_works_count: usize) {
        self.spawn_flush_worker();

        for _ in 0..self
            .flush_manager
            .read()
            .expect("lock is poisoned")
            .queues
            .len()
        {
            self.flush_semaphore.release();
        }

        log::info!("Spawning {compaction_works_count} compaction threads");
        for _ in 0..compaction_works_count {
            self.spawn_compaction_worker();
        }

        if let Some(ms) = self.config.fsync_ms {
            self.spawn_fsync_thread(ms.into());
        }

        self.spawn_monitor_thread();
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

        #[cfg(not(target_os = "windows"))]
        {
            // fsync folder on Unix
            let folder = File::open(&partition_path)?;
            folder.sync_all()?;
        }

        self.flush_manager
            .write()
            .expect("lock is poisoned")
            .remove_partition(&handle.name);

        self.compaction_manager.remove_partition(&handle.name);

        self.partitions
            .write()
            .expect("lock is poisoned")
            .remove(&handle.name);

        std::fs::remove_dir_all(partition_path)?;

        Ok(())
    }

    /// Creates or opens a keyspace partition.
    ///
    /// # Errors
    ///
    /// Returns error, if an IO error occured.
    ///
    /// # Panics
    ///
    /// Panics if the partition name includes characters other than: a-z A-Z 0-9 _ -
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

            handle
        })
    }

    /// Gets the list of all partitions in the keyspace
    #[must_use]
    pub fn list_partitions(&self) -> Vec<PartitionKey> {
        self.partitions
            .read()
            .expect("lock is poisoned")
            .keys()
            .cloned()
            .collect()
    }

    /// Returns `true` if the partition with the given name exists
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
        self.seqno.get()
    }

    fn check_version<P: AsRef<Path>>(path: P) -> crate::Result<()> {
        let bytes = std::fs::read(path.as_ref().join(FJALL_MARKER))?;

        if let Some(version) = Version::parse_file_header(&bytes) {
            if version != Version::V0 {
                return Err(crate::Error::InvalidVersion(Some(version)));
            }
        } else {
            return Err(crate::Error::InvalidVersion(None));
        }

        Ok(())
    }

    fn find_active_journal<P: AsRef<Path>>(
        path: P,
        recovery_mode: RecoveryMode,
    ) -> crate::Result<Option<(Journal, HashMap<PartitionKey, MemTable>)>> {
        let mut journal = None;

        for dirent in std::fs::read_dir(path)? {
            let dirent = dirent?;

            if !dirent.path().join(FLUSH_MARKER).try_exists()? {
                journal = Some(Journal::recover(dirent.path(), recovery_mode)?);
            }
        }

        Ok(journal)
    }

    /// Recovers existing keyspace from directory
    #[allow(clippy::too_many_lines)]
    #[doc(hidden)]
    pub fn recover(config: Config) -> crate::Result<Self> {
        log::info!("Recovering keyspace at {}", config.path.display());
        let recovery_mode = config.journal_recovery_mode;

        // Check version
        Self::check_version(&config.path)?;

        // Get active journal if it exists
        let journals_folder = config.path.join(JOURNALS_FOLDER);
        let active_journal = Self::find_active_journal(&journals_folder, recovery_mode)?;

        let (journal, mut memtables) = if let Some((journal, memtables)) = active_journal {
            log::debug!("Recovered active journal at {}", journal.path.display());
            (journal, memtables)
        } else {
            let journal = Journal::create_new(journals_folder.join(&*generate_segment_id()))?;
            let memtables = HashMap::default();
            (journal, memtables)
        };

        let journal = Arc::new(journal);
        let journal_path = journal.path.clone();

        let journal_manager = JournalManager::new(journal_path);

        // Construct (empty) keyspace, then fill back with partition data
        let inner = KeyspaceInner {
            config,
            journal,
            partitions: Arc::new(RwLock::new(Partitions::with_capacity(10))),
            seqno: SequenceNumberCounter::default(),
            flush_manager: Arc::default(),
            journal_manager: Arc::new(RwLock::new(journal_manager)),
            flush_semaphore: Arc::new(Semaphore::new(0)),
            compaction_manager: CompactionManager::default(),
            stop_signal: lsm_tree::stop_signal::StopSignal::default(),
            active_background_threads: Arc::default(),
            write_buffer_manager: WriteBufferManager::default(),
        };

        let keyspace = Self(Arc::new(inner));

        // Recover partitions
        recover_partitions(&keyspace, &mut memtables)?;

        // Recover sealed memtables by walking through old journals
        recover_sealed_memtables(&keyspace)?;

        Ok(keyspace)
    }

    #[doc(hidden)]
    pub fn create_new(config: Config) -> crate::Result<Self> {
        let path = config.path.clone();
        log::info!("Creating keyspace at {}", path.display());

        std::fs::create_dir_all(&path)?;

        let marker_path = path.join(FJALL_MARKER);
        assert!(!marker_path.try_exists()?);

        std::fs::create_dir_all(path.join(JOURNALS_FOLDER))?;
        std::fs::create_dir_all(path.join(PARTITIONS_FOLDER))?;

        let active_journal_path = path.join(JOURNALS_FOLDER).join(&*generate_segment_id());
        let journal = Journal::create_new(&active_journal_path)?;
        let journal = Arc::new(journal);

        let inner = KeyspaceInner {
            config,
            journal,
            partitions: Arc::new(RwLock::new(Partitions::with_capacity(10))),
            seqno: SequenceNumberCounter::default(),
            flush_manager: Arc::default(),
            journal_manager: Arc::new(RwLock::new(JournalManager::new(active_journal_path))),
            flush_semaphore: Arc::new(Semaphore::new(0)),
            compaction_manager: CompactionManager::default(),
            stop_signal: lsm_tree::stop_signal::StopSignal::default(),
            active_background_threads: Arc::default(),
            write_buffer_manager: WriteBufferManager::default(),
        };

        // NOTE: Lastly, fsync .fjall marker, which contains the version
        // -> the keyspace is fully initialized
        let mut file = std::fs::File::create(marker_path)?;
        Version::V0.write_file_header(&mut file)?;
        file.sync_all()?;

        #[cfg(not(target_os = "windows"))]
        {
            // fsync folders on Unix

            let folder = std::fs::File::open(path.join(JOURNALS_FOLDER))?;
            folder.sync_all()?;

            let folder = std::fs::File::open(path.join(PARTITIONS_FOLDER))?;
            folder.sync_all()?;

            let folder = std::fs::File::open(&path)?;
            folder.sync_all()?;
        }

        Ok(Self(Arc::new(inner)))
    }

    fn spawn_monitor_thread(&self) {
        let monitor = Monitor::new(self);
        let stop_signal = self.stop_signal.clone();
        let thread_counter = self.active_background_threads.clone();

        thread_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        std::thread::spawn(move || {
            while !stop_signal.is_stopped() {
                let idle = monitor.run();

                if idle {
                    std::thread::sleep(std::time::Duration::from_millis(250));
                }
            }

            log::trace!("monitor: exiting because tree is dropping");
            thread_counter.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
        });
    }

    fn spawn_fsync_thread(&self, ms: usize) {
        let journal = self.journal.clone();
        let stop_signal = self.stop_signal.clone();

        std::thread::spawn(move || {
            while !stop_signal.is_stopped() {
                log::trace!("fsync thread: sleeping {ms}ms");
                std::thread::sleep(std::time::Duration::from_millis(ms as u64));

                log::trace!("fsync thread: fsycing journal");
                if let Err(e) = journal.flush(false) {
                    log::error!("Fsync failed: {e:?}");
                }
            }

            log::debug!("fsync thread: exiting because tree is dropping");
        });
    }

    fn spawn_compaction_worker(&self) {
        let compaction_manager = self.compaction_manager.clone();
        let stop_signal = self.stop_signal.clone();
        let thread_counter = self.active_background_threads.clone();

        thread_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        std::thread::spawn(move || {
            while !stop_signal.is_stopped() {
                log::debug!("compaction: waiting for work");
                compaction_manager.wait_for();

                crate::compaction::worker::run(&compaction_manager);
            }

            log::debug!("compaction thread: exiting because tree is dropping");
            thread_counter.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
        });
    }

    fn spawn_flush_worker(&self) {
        let flush_manager = self.flush_manager.clone();
        let journal_manager = self.journal_manager.clone();
        let compaction_manager = self.compaction_manager.clone();
        let flush_semaphore = self.flush_semaphore.clone();
        let write_buffer_manager = self.write_buffer_manager.clone();

        let thread_counter = self.active_background_threads.clone();
        let stop_signal = self.stop_signal.clone();

        thread_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        std::thread::spawn(move || {
            while !stop_signal.is_stopped() {
                log::trace!("flush worker: acquiring flush semaphore");
                flush_semaphore.acquire();

                crate::flush::worker::run(
                    &flush_manager,
                    &journal_manager,
                    &compaction_manager,
                    &write_buffer_manager,
                );
            }

            log::trace!("flush worker: exiting because tree is dropping");
            thread_counter.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
        });
    }
}
