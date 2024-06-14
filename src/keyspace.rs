use crate::{
    batch::{Batch, PartitionKey},
    compaction::manager::CompactionManager,
    config::Config,
    file::{
        fsync_directory, FJALL_MARKER, FLUSH_MARKER, JOURNALS_FOLDER, PARTITIONS_FOLDER,
        PARTITION_DELETED_MARKER,
    },
    flush::manager::FlushManager,
    journal::{manager::JournalManager, shard::RecoveryMode, writer::PersistMode, Journal},
    monitor::Monitor,
    partition::name::is_valid_partition_name,
    recovery::{recover_partitions, recover_sealed_memtables},
    version::Version,
    write_buffer_manager::WriteBufferManager,
    PartitionCreateOptions, PartitionHandle,
};
use lsm_tree::{MemTable, SequenceNumberCounter};
use std::{
    collections::HashMap,
    fs::File,
    path::Path,
    sync::{
        atomic::{AtomicBool, AtomicUsize},
        Arc, RwLock,
    },
};
use std_semaphore::Semaphore;

pub type Partitions = HashMap<PartitionKey, PartitionHandle>;

#[allow(clippy::module_name_repetitions)]
pub struct KeyspaceInner {
    /// Dictionary of all partitions
    pub(crate) partitions: Arc<RwLock<Partitions>>,

    /// Journal (write-ahead-log/WAL)
    pub(crate) journal: Arc<Journal>,

    /// Keyspace configuration
    pub(crate) config: Config,

    /// Current sequence number
    pub(crate) seqno: SequenceNumberCounter,

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
}

impl Drop for KeyspaceInner {
    fn drop(&mut self) {
        log::trace!("Dropping Keyspace, trying to flush journal");

        self.stop_signal.send();

        match self.journal.flush(PersistMode::SyncAll) {
            Ok(()) => {
                log::trace!("Flushed journal successfully");
            }
            Err(e) => {
                log::error!("Flush error on drop: {e:?}");
            }
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
        // TODO: maybe allow setting a custom capacity
        Batch::with_capacity(self.clone(), 10)
    }

    /// Returns the current write buffer size (active + sealed memtables).
    #[must_use]
    pub fn write_buffer_size(&self) -> u64 {
        self.write_buffer_manager.get()
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
    pub fn journal_disk_space(&self) -> u64 {
        self.journal_manager
            .read()
            .expect("lock is poisoned")
            .disk_space_used()
    }

    /// Returns approximate memory usage.
    pub fn memory_usage(&self) -> u64 {
        self.0.config.block_cache.size() + self.write_buffer_size()
        // TODO: 2.0.0 + blob cache
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
    /// assert!(keyspace.disk_space() >= 0);
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

    /// Flushes the active journal to OS buffers. The durability depends on the [`PersistMode`]
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
    /// Returns error, if an IO error occured.
    pub fn persist(&self, mode: PersistMode) -> crate::Result<()> {
        if self.is_poisoned.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(crate::Error::Poisoned);
        }

        if let Err(e) = self.journal.flush(mode) {
            self.is_poisoned
                .store(true, std::sync::atomic::Ordering::Release);
            log::error!(
                "flush failed, which is a FATAL, and possibly hardware-related, failure: {e:?}"
            );
            return Err(crate::Error::Poisoned);
        };
        Ok(())
    }

    /// Opens a keyspace in the given directory.
    ///
    /// # Errors
    ///
    /// Returns error, if an IO error occured.
    pub fn open(config: Config) -> crate::Result<Self> {
        let keyspace = Self::create_or_recover(config)?;
        keyspace.start_background_threads();
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
    pub(crate) fn start_background_threads(&self) {
        if self.config.flush_workers_count > 0 {
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
        }

        log::debug!(
            "Spawning {} compaction threads",
            self.config.compaction_workers_count
        );
        for _ in 0..self.config.compaction_workers_count {
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

        // IMPORTANT: fsync folder on Unix
        fsync_directory(&partition_path)?;

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
    /// Partition names can be up to 255 characters long, can not be empty and
    /// can only contain alphanumerics, underscore (`_`) and dash (`-`).
    ///
    /// # Errors
    ///
    /// Returns error, if an IO error occured.
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

            handle
        })
    }

    /// Returns the amount of partitions
    #[must_use]
    pub fn partition_count(&self) -> usize {
        self.partitions.read().expect("lock is poisoned").len()
    }

    /// Gets a list of all partition names in the keyspace
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
        self.seqno.get()
    }

    fn check_version<P: AsRef<Path>>(path: P) -> crate::Result<()> {
        let bytes = std::fs::read(path.as_ref().join(FJALL_MARKER))?;

        if let Some(version) = Version::parse_file_header(&bytes) {
            if version != Version::V1 {
                return Err(crate::Error::InvalidVersion(Some(version)));
            }
        } else {
            return Err(crate::Error::InvalidVersion(None));
        }

        Ok(())
    }

    // TODO: create struct for return type :!
    #[allow(clippy::type_complexity)]
    fn find_active_journal<P: AsRef<Path>>(
        path: P,
        recovery_mode: RecoveryMode,
    ) -> crate::Result<(
        lsm_tree::SegmentId,
        Option<(Journal, HashMap<PartitionKey, MemTable>)>,
    )> {
        let mut journal = None;
        let mut max_journal_id = 0;

        for dirent in std::fs::read_dir(path)? {
            let dirent = dirent?;

            let journal_id = dirent
                .file_name()
                .to_str()
                .expect("should be utf-8")
                .parse::<lsm_tree::SegmentId>()
                .expect("should be valid journal ID");

            max_journal_id = max_journal_id.max(journal_id);

            if !dirent.path().join(FLUSH_MARKER).try_exists()? {
                journal = Some(Journal::recover(dirent.path(), recovery_mode)?);
            }
        }

        Ok((max_journal_id, journal))
    }

    /// Recovers existing keyspace from directory.
    #[allow(clippy::too_many_lines)]
    #[doc(hidden)]
    pub fn recover(config: Config) -> crate::Result<Self> {
        log::info!("Recovering keyspace at {:?}", config.path);
        let recovery_mode = config.journal_recovery_mode;

        // Check version
        Self::check_version(&config.path)?;

        // Get active journal if it exists
        let journals_folder = config.path.join(JOURNALS_FOLDER);
        let (max_journal_id, active_journal) =
            Self::find_active_journal(&journals_folder, recovery_mode)?;

        let (journal, mut memtables) = if let Some((journal, memtables)) = active_journal {
            log::debug!("Recovered active journal at {:?}", journal.path);
            (journal, memtables)
        } else {
            let journal =
                Journal::create_new(journals_folder.join((max_journal_id + 1).to_string()))?;

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
            is_poisoned: Arc::default(),
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
        log::info!("Creating keyspace at {path:?}");

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
            partitions: Arc::new(RwLock::new(Partitions::with_capacity(10))),
            seqno: SequenceNumberCounter::default(),
            flush_manager: Arc::default(),
            journal_manager: Arc::new(RwLock::new(JournalManager::new(active_journal_path))),
            flush_semaphore: Arc::new(Semaphore::new(0)),
            compaction_manager: CompactionManager::default(),
            stop_signal: lsm_tree::stop_signal::StopSignal::default(),
            active_background_threads: Arc::default(),
            write_buffer_manager: WriteBufferManager::default(),
            is_poisoned: Arc::default(),
        };

        // NOTE: Lastly, fsync .fjall marker, which contains the version
        // -> the keyspace is fully initialized
        let mut file = std::fs::File::create(marker_path)?;
        Version::V1.write_file_header(&mut file)?;
        file.sync_all()?;

        // IMPORTANT: fsync folders on Unix
        fsync_directory(&journal_folder_path)?;
        fsync_directory(&partition_folder_path)?;
        fsync_directory(&path)?;

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
        let is_poisoned = self.is_poisoned.clone();

        std::thread::spawn(move || {
            while !stop_signal.is_stopped() {
                log::trace!("fsync thread: sleeping {ms}ms");
                std::thread::sleep(std::time::Duration::from_millis(ms as u64));

                log::trace!("fsync thread: fsycing journal");
                if let Err(e) = journal.flush(PersistMode::SyncAll) {
                    is_poisoned.store(true, std::sync::atomic::Ordering::Release);
                    log::error!(
                        "flush failed, which is a FATAL, and possibly hardware-related, failure: {e:?}"
                    );
                    return;
                }
            }

            log::trace!("fsync thread: exiting because tree is dropping");
        });
    }

    fn spawn_compaction_worker(&self) {
        let compaction_manager = self.compaction_manager.clone();
        let stop_signal = self.stop_signal.clone();
        let thread_counter = self.active_background_threads.clone();

        thread_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        std::thread::spawn(move || {
            while !stop_signal.is_stopped() {
                log::trace!("compaction: waiting for work");
                compaction_manager.wait_for();

                crate::compaction::worker::run(&compaction_manager);
            }

            log::trace!("compaction thread: exiting because tree is dropping");
            thread_counter.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
        });
    }

    /// Only used for internal testing.
    ///
    /// Should NOT be called when there is a flush worker active already!!!
    #[doc(hidden)]
    pub fn force_flush(&self) {
        let parallelism = self.config.flush_workers_count;

        crate::flush::worker::run(
            &self.flush_manager,
            &self.journal_manager,
            &self.compaction_manager,
            &self.write_buffer_manager,
            parallelism,
        );
    }

    fn spawn_flush_worker(&self) {
        let flush_manager = self.flush_manager.clone();
        let journal_manager = self.journal_manager.clone();
        let compaction_manager = self.compaction_manager.clone();
        let flush_semaphore = self.flush_semaphore.clone();
        let write_buffer_manager = self.write_buffer_manager.clone();

        let thread_counter = self.active_background_threads.clone();
        let stop_signal = self.stop_signal.clone();

        let parallelism = self.config.flush_workers_count;

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
                    parallelism,
                );
            }

            log::trace!("flush worker: exiting because tree is dropping");
            thread_counter.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_log::test;

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

        keyspace.force_flush();

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

        keyspace.force_flush();

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
