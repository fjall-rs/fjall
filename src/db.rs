// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    background_worker::{Activity, BackgroundWorker},
    batch::{Batch, KeyspaceKey},
    compaction::manager::CompactionManager,
    config::Config,
    file::{
        fsync_directory, FJALL_MARKER, JOURNALS_FOLDER, KEYSPACES_FOLDER, KEYSPACE_DELETED_MARKER,
        LOCK_FILE,
    },
    flush::manager::FlushManager,
    journal::{manager::JournalManager, writer::PersistMode, Journal},
    keyspace::name::is_valid_keyspace_name,
    monitor::Monitor,
    poison_dart::PoisonDart,
    recovery::{recover_keyspaces, recover_sealed_memtables},
    snapshot_tracker::SnapshotTracker,
    stats::Stats,
    version::Version,
    write_buffer_manager::WriteBufferManager,
    HashMap, Keyspace, KeyspaceCreateOptions,
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

pub type Keyspaces = HashMap<KeyspaceKey, Keyspace>;

#[allow(clippy::module_name_repetitions)]
pub struct DatabaseInner {
    /// Dictionary of all keyspaces
    #[doc(hidden)]
    pub keyspaces: Arc<RwLock<Keyspaces>>,

    /// Journal (write-ahead-log/WAL)
    pub(crate) journal: Arc<Journal>,

    /// Database configuration
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

    /// Keeps track of which keyspaces are most likely to be
    /// candidates for compaction
    pub(crate) compaction_manager: CompactionManager,

    /// Stop signal when database is dropped to stop background threads
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

    lock_fd: File,
}

impl Drop for DatabaseInner {
    fn drop(&mut self) {
        log::trace!("Dropping Database");

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
        self.keyspaces.write().expect("lock is poisoned").clear();
        self.journal_manager
            .write()
            .expect("lock is poisoned")
            .clear();

        self.config.descriptor_table.clear();

        if self.config.clean_path_on_drop {
            log::info!(
                "Deleting database because temporary=true: {}",
                self.config.path.display(),
            );

            if let Err(err) = remove_dir_all(&self.config.path) {
                log::warn!(
                    "Failed to clean up path: {} - {err}",
                    self.config.path.display()
                );
            }
        }

        // Unlock for good measure
        if let Err(e) = self.lock_fd.unlock() {
            log::warn!("Failed to unlock lock file: {e}");
        }

        #[cfg(feature = "__internal_whitebox")]
        crate::drop::decrement_drop_counter();
    }
}

/// A database is a single logical database
/// which can house multiple keyspaces
///
/// In your application, you should create a single database
/// and keep it around for as long as needed
/// (as long as you are using its keyspaces).
#[derive(Clone)]
#[doc(alias = "database")]
#[doc(alias = "collection")]
pub struct Database(pub(crate) Arc<DatabaseInner>);

impl std::ops::Deref for Database {
    type Target = DatabaseInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Database {
    /// Creates a new database builder to create or open a database at `path`.
    pub fn builder(path: impl AsRef<Path>) -> crate::DatabaseBuilder {
        crate::DatabaseBuilder::new(path.as_ref())
    }

    /// Initializes a new atomic write batch.
    ///
    /// Items may be written to multiple keyspaces, which
    /// will be be updated atomically when the batch is committed.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Database, KeyspaceCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = Database::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
    /// let mut batch = db.batch();
    ///
    /// assert_eq!(tree.len()?, 0);
    /// batch.insert(&tree, "1", "abc");
    /// batch.insert(&tree, "3", "abc");
    /// batch.insert(&tree, "5", "abc");
    ///
    /// assert_eq!(tree.len()?, 0);
    ///
    /// batch.commit()?;
    /// assert_eq!(tree.len()?, 3);
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
    /// # use fjall::{Database, KeyspaceCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = Database::builder(folder).open()?;
    /// assert_eq!(1, db.journal_count());
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

    /// Returns the disk space usage of the entire database.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Database, KeyspaceCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = Database::builder(folder).open()?;
    /// # let _tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
    /// assert!(db.disk_space() > 0);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    pub fn disk_space(&self) -> u64 {
        let keyspaces_size = self
            .keyspaces
            .read()
            .expect("lock is poisoned")
            .values()
            .map(Keyspace::disk_space)
            .sum::<u64>();

        self.journal_disk_space() + keyspaces_size
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
    /// # use fjall::{PersistMode, Database, KeyspaceCreateOptions};
    /// # let folder = tempfile::tempdir()?;
    /// let db = Database::builder(folder).open()?;
    /// let items = db.keyspace("my_items", KeyspaceCreateOptions::default())?;
    ///
    /// items.insert("a", "hello")?;
    ///
    /// db.persist(PersistMode::SyncAll)?;
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

    /// Opens a database in the given directory.
    ///
    /// # Errors
    ///
    /// Returns error, if an IO error occurred.
    pub fn open(config: Config) -> crate::Result<Self> {
        log::debug!(
            "cache capacity={}MiB",
            config.cache.capacity() / 1_024 / 1_024,
        );

        let db = Self::create_or_recover(config)?;
        db.start_background_threads()?;

        #[cfg(feature = "__internal_whitebox")]
        crate::drop::increment_drop_counter();

        Ok(db)
    }

    /// Same as [`Database::open`], but does not start background threads.
    ///
    /// Needed to open a database without threads for testing.
    ///
    /// Should not be user-facing.
    #[doc(hidden)]
    pub fn create_or_recover(config: Config) -> crate::Result<Self> {
        if config.path.join(FJALL_MARKER).try_exists()? {
            Self::recover(config)
        } else {
            Self::create_new(config)
        }
    }

    /// Starts background threads that maintain the database.
    ///
    /// Should not be called, unless in [`Database::open`]
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

    /// Destroys the keyspace, removing all data associated with it.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn delete_keyspace(&self, handle: Keyspace) -> crate::Result<()> {
        let keyspace_path = handle.path();

        // TODO: just remove from meta keyspace instead
        let file = File::create_new(keyspace_path.join(KEYSPACE_DELETED_MARKER))?;
        file.sync_all()?;

        // IMPORTANT: fsync folder on Unix
        fsync_directory(keyspace_path)?;

        handle
            .is_deleted
            .store(true, std::sync::atomic::Ordering::Release);

        // IMPORTANT: Care, locks keyspaces map
        self.compaction_manager.remove_keyspace(&handle.name);

        self.flush_manager
            .write()
            .expect("lock is poisoned")
            .remove_keyspace(&handle.name);

        self.keyspaces
            .write()
            .expect("lock is poisoned")
            .remove(&handle.name);

        Ok(())
    }

    /// Creates or opens a keyspace.
    ///
    /// If the keyspace does not yet exist, it will be created configured with `create_options`.
    /// Otherwise simply a handle to the existing keyspace will be returned.
    ///
    /// Keyspace names can be up to 255 characters long and can not be empty.
    ///
    /// # Errors
    ///
    /// Returns error, if an IO error occurred.
    ///
    /// # Panics
    ///
    /// Panics if the keyspace name is invalid.
    pub fn keyspace(
        &self,
        name: &str,
        create_options: KeyspaceCreateOptions,
    ) -> crate::Result<Keyspace> {
        assert!(is_valid_keyspace_name(name));

        let mut keyspaces = self.keyspaces.write().expect("lock is poisoned");

        Ok(if let Some(keyspace) = keyspaces.get(name) {
            keyspace.clone()
        } else {
            let name: KeyspaceKey = name.into();

            let handle = Keyspace::create_new(self, name.clone(), create_options)?;
            keyspaces.insert(name, handle.clone());

            #[cfg(feature = "__internal_whitebox")]
            crate::drop::increment_drop_counter();

            handle
        })
    }

    /// Returns the number of keyspaces.
    #[must_use]
    pub fn keyspace_count(&self) -> usize {
        self.keyspaces.read().expect("lock is poisoned").len()
    }

    /// Gets a list of all keyspace names in the database.
    #[must_use]
    pub fn list_keyspaces(&self) -> Vec<KeyspaceKey> {
        self.keyspaces
            .read()
            .expect("lock is poisoned")
            .keys()
            .cloned()
            .collect()
    }

    /// Returns `true` if the keyspace with the given name exists.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Database, KeyspaceCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = Database::builder(folder).open()?;
    /// assert!(!db.keyspace_exists("default"));
    /// db.keyspace("default", KeyspaceCreateOptions::default())?;
    /// assert!(db.keyspace_exists("default"));
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    #[must_use]
    pub fn keyspace_exists(&self, name: &str) -> bool {
        self.keyspaces
            .read()
            .expect("lock is poisoned")
            .contains_key(name)
    }

    /// Gets the current sequence number.
    ///
    /// Can be used to start a cross-keyspace snapshot, using [`Keyspace::snapshot_at`].
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Database, KeyspaceCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = Database::builder(folder).open()?;
    /// let tree1 = db.keyspace("default", KeyspaceCreateOptions::default())?;
    /// let tree2 = db.keyspace("another", KeyspaceCreateOptions::default())?;
    ///
    /// tree1.insert("abc1", "abc")?;
    /// tree2.insert("abc2", "abc")?;
    ///
    /// let instant = db.instant();
    /// let snapshot1 = tree1.snapshot_at(instant);
    /// let snapshot2 = tree2.snapshot_at(instant);
    ///
    /// assert!(tree1.contains_key("abc1")?);
    /// assert!(tree2.contains_key("abc2")?);
    ///
    /// assert!(snapshot1.contains_key("abc1")?);
    /// assert!(snapshot2.contains_key("abc2")?);
    ///
    /// tree1.insert("def1", "def")?;
    /// tree2.insert("def2", "def")?;
    ///
    /// assert!(!snapshot1.contains_key("def1")?);
    /// assert!(!snapshot2.contains_key("def2")?);
    ///
    /// assert!(tree1.contains_key("def1")?);
    /// assert!(tree2.contains_key("def2")?);
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
            if version != Version::V3 {
                return Err(crate::Error::InvalidVersion(Some(version)));
            }
        } else {
            return Err(crate::Error::InvalidVersion(None));
        }

        Ok(())
    }

    /// Recovers existing database from directory.
    #[allow(clippy::too_many_lines)]
    #[doc(hidden)]
    pub fn recover(config: Config) -> crate::Result<Self> {
        log::info!("Recovering database at {}", config.path.display());

        // Check version
        Self::check_version(&config.path)?;

        let lock_file = File::open(config.path.join(LOCK_FILE))?;
        lock_file.try_lock().map_err(|e| match e {
            std::fs::TryLockError::Error(e) => crate::Error::Io(e),
            std::fs::TryLockError::WouldBlock => crate::Error::Locked,
        })?;

        // TODO:
        // let recovery_mode = config.journal_recovery_mode;

        // Reload active journal
        let journals_folder = config.path.join(JOURNALS_FOLDER);
        let journal_recovery = Journal::recover(journals_folder)?;
        log::debug!("journal recovery result: {journal_recovery:#?}");

        let active_journal = Arc::new(journal_recovery.active);
        active_journal.get_writer().persist(PersistMode::SyncAll)?;

        let sealed_journals = journal_recovery.sealed;

        let journal_manager = JournalManager::from_active(active_journal.path());

        // Construct (empty) database, then fill back with keyspace data
        let inner = DatabaseInner {
            config,
            journal: active_journal,
            keyspaces: Arc::new(RwLock::new(Keyspaces::with_capacity_and_hasher(
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
            lock_fd: lock_file,
        };

        let db = Self(Arc::new(inner));

        // Recover keyspaces
        recover_keyspaces(&db)?;

        // Recover sealed memtables by walking through old journals
        recover_sealed_memtables(
            &db,
            &sealed_journals
                .into_iter()
                .map(|(_, x)| x)
                .collect::<Vec<_>>(),
        )?;

        {
            let keyspaces = db.keyspaces.read().expect("lock is poisoned");

            #[cfg(debug_assertions)]
            for keyspace in keyspaces.values() {
                // NOTE: If this triggers, the last sealed memtable
                // was not correctly rotated
                debug_assert!(
                    keyspace.tree.lock_active_memtable().is_empty(),
                    "active memtable is not empty - this is a bug"
                );
            }

            // NOTE: We only need to recover the active journal, if it actually existed before
            // nothing to recover, if we just created it
            if !journal_recovery.was_active_created {
                log::trace!("Recovering active memtables from active journal");

                let reader = db.journal.get_reader()?;

                for batch in reader {
                    let batch = batch?;

                    for item in batch.items {
                        if let Some(keyspace) = keyspaces.get(&item.keyspace) {
                            let tree = &keyspace.tree;

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

                for keyspace in keyspaces.values() {
                    let size = keyspace.tree.active_memtable_size();

                    log::trace!(
                        "Recovered active memtable of size {size}B for keyspace {:?} ({} items)",
                        keyspace.name,
                        keyspace.tree.lock_active_memtable().len(),
                    );

                    // IMPORTANT: Add active memtable size to current write buffer size
                    db.write_buffer_manager.allocate(size);

                    // Recover seqno
                    let maybe_next_seqno = keyspace
                        .tree
                        .get_highest_seqno()
                        .map(|x| x + 1)
                        .unwrap_or_default();

                    db.seqno.fetch_max(maybe_next_seqno);
                    log::debug!("Database seqno is now {}", db.seqno.get());
                }
            }
        }

        db.visible_seqno.set(db.seqno.get());

        log::trace!("Recovery successful");

        Ok(db)
    }

    #[doc(hidden)]
    pub fn create_new(config: Config) -> crate::Result<Self> {
        log::info!("Creating database at {}", config.path.display());

        std::fs::create_dir_all(&config.path)?;

        let lock_file = File::create_new(config.path.join(LOCK_FILE))?;
        lock_file.try_lock().map_err(|e| match e {
            std::fs::TryLockError::Error(e) => crate::Error::Io(e),
            std::fs::TryLockError::WouldBlock => crate::Error::Locked,
        })?;

        let journal_folder_path = config.path.join(JOURNALS_FOLDER);
        let keyspace_folder_path = config.path.join(KEYSPACES_FOLDER);

        std::fs::create_dir_all(&journal_folder_path)?;
        std::fs::create_dir_all(&keyspace_folder_path)?;

        let active_journal_path = journal_folder_path.join("0");
        let journal = Journal::create_new(&active_journal_path)?;
        let journal = Arc::new(journal);

        // NOTE: Lastly, fsync .fjall marker, which contains the version
        let mut marker = std::fs::File::create_new(config.path.join(FJALL_MARKER))?;
        Version::V3.write_file_header(&mut marker)?;
        marker.sync_all()?;

        // IMPORTANT: fsync folders on Unix
        fsync_directory(&journal_folder_path)?;
        fsync_directory(&keyspace_folder_path)?;
        fsync_directory(&config.path)?;

        let inner = DatabaseInner {
            config,
            journal,
            keyspaces: Arc::new(RwLock::new(Keyspaces::with_capacity_and_hasher(
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
            lock_fd: lock_file,
        };

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
#[allow(clippy::default_trait_access, clippy::expect_used)]
mod tests {
    use super::*;
    use test_log::test;

    // TODO: 3.0.0 if we store the keyspace as a monotonic integer
    // and the keyspace's name inside the keyspace options/manifest
    // we could allow all UTF-8 characters for keyspace names
    //
    // https://github.com/fjall-rs/fjall/issues/89
    #[test]
    #[ignore = "remove"]
    pub fn test_exotic_keyspace_names() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;
        let db = Database::builder(&folder).open()?;

        for name in ["hello$world", "hello#world", "hello.world", "hello_world"] {
            let tree = db.keyspace(name, Default::default())?;
            tree.insert("a", "a")?;
            assert_eq!(1, tree.len()?);
        }

        Ok(())
    }

    #[test]
    pub fn recover_after_rotation_multiple_keyspaces() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;

        {
            let db = Database::create_or_recover(Config::new(folder.path()))?;
            let tree = db.keyspace("default", Default::default())?;
            let tree2 = db.keyspace("default2", Default::default())?;

            tree.insert("a", "a")?;
            tree2.insert("a", "a")?;
            assert_eq!(1, tree.len()?);
            assert_eq!(1, tree2.len()?);

            assert_eq!(None, tree.tree.get_highest_persisted_seqno());
            assert_eq!(None, tree2.tree.get_highest_persisted_seqno());

            tree.rotate_memtable()?;

            assert_eq!(1, tree.len()?);
            assert_eq!(1, tree.tree.sealed_memtable_count());

            assert_eq!(1, tree2.len()?);
            assert_eq!(0, tree2.tree.sealed_memtable_count());

            tree2.insert("b", "b")?;
            tree2.rotate_memtable()?;

            assert_eq!(1, tree.len()?);
            assert_eq!(1, tree.tree.sealed_memtable_count());

            assert_eq!(2, tree2.len()?);
            assert_eq!(1, tree2.tree.sealed_memtable_count());
        }

        {
            // IMPORTANT: We need to allocate enough flush workers
            // because on CI there may not be enough cores by default
            // so the result would be wrong
            let config = Database::builder(&folder).flush_workers(16).into_config();
            let db = Database::create_or_recover(config)?;

            let tree = db.keyspace("default", Default::default())?;
            let tree2 = db.keyspace("default2", Default::default())?;

            assert_eq!(1, tree.len()?);
            assert_eq!(1, tree.tree.sealed_memtable_count());

            assert_eq!(2, tree2.len()?);
            assert_eq!(2, tree2.tree.sealed_memtable_count());

            assert_eq!(3, db.journal_count());

            db.force_flush()?;

            assert_eq!(1, tree.len()?);
            assert_eq!(0, tree.tree.sealed_memtable_count());

            assert_eq!(2, tree2.len()?);
            assert_eq!(0, tree2.tree.sealed_memtable_count());

            assert_eq!(1, db.journal_count());

            assert_eq!(Some(0), tree.tree.get_highest_persisted_seqno());
            assert_eq!(Some(2), tree2.tree.get_highest_persisted_seqno());
        }

        Ok(())
    }

    #[test]
    pub fn recover_after_rotation() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;

        {
            let db = Database::create_or_recover(Config::new(folder.path()))?;
            let tree = db.keyspace("default", Default::default())?;

            tree.insert("a", "a")?;
            assert_eq!(1, tree.len()?);

            tree.rotate_memtable()?;

            assert_eq!(1, tree.len()?);
            assert_eq!(1, tree.tree.sealed_memtable_count());
        }

        {
            let db = Database::create_or_recover(Config::new(folder.path()))?;
            let tree = db.keyspace("default", Default::default())?;

            assert_eq!(1, tree.len()?);
            assert_eq!(1, tree.tree.sealed_memtable_count());
            assert_eq!(2, db.journal_count());

            db.force_flush()?;

            assert_eq!(1, tree.len()?);
            assert_eq!(0, tree.tree.sealed_memtable_count());
            assert_eq!(1, db.journal_count());
        }

        Ok(())
    }

    #[test]
    pub fn force_flush_multiple_keyspaces() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;

        let db = Database::create_or_recover(Config::new(folder.path()))?;
        let tree = db.keyspace("default", Default::default())?;
        let tree2 = db.keyspace("default2", Default::default())?;

        assert_eq!(0, db.write_buffer_size());

        assert_eq!(0, tree.segment_count());
        assert_eq!(0, tree2.segment_count());

        assert_eq!(
            0,
            db.journal_manager
                .read()
                .expect("lock is poisoned")
                .sealed_journal_count()
        );

        assert_eq!(
            0,
            db.flush_manager
                .read()
                .expect("lock is poisoned")
                .queued_size()
        );

        assert_eq!(0, db.flush_manager.read().expect("lock is poisoned").len());

        for _ in 0..100 {
            tree.insert(nanoid::nanoid!(), "abc")?;
            tree2.insert(nanoid::nanoid!(), "abc")?;
        }

        tree.rotate_memtable()?;

        assert_eq!(1, db.flush_manager.read().expect("lock is poisoned").len());

        assert_eq!(
            1,
            db.journal_manager
                .read()
                .expect("lock is poisoned")
                .sealed_journal_count()
        );

        for _ in 0..100 {
            tree2.insert(nanoid::nanoid!(), "abc")?;
        }

        tree2.rotate_memtable()?;

        assert_eq!(2, db.flush_manager.read().expect("lock is poisoned").len());

        assert_eq!(
            2,
            db.journal_manager
                .read()
                .expect("lock is poisoned")
                .sealed_journal_count()
        );

        assert_eq!(0, tree.segment_count());
        assert_eq!(0, tree2.segment_count());

        db.force_flush()?;

        assert_eq!(
            0,
            db.flush_manager
                .read()
                .expect("lock is poisoned")
                .queued_size()
        );

        assert_eq!(0, db.flush_manager.read().expect("lock is poisoned").len());

        assert_eq!(
            0,
            db.journal_manager
                .read()
                .expect("lock is poisoned")
                .sealed_journal_count()
        );

        assert_eq!(0, db.write_buffer_size());
        assert_eq!(1, tree.segment_count());
        assert_eq!(1, tree2.segment_count());

        Ok(())
    }

    #[test]
    pub fn force_flush() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;

        let db = Database::create_or_recover(Config::new(folder.path()))?;
        let tree = db.keyspace("default", Default::default())?;

        assert_eq!(0, db.write_buffer_size());

        assert_eq!(0, tree.segment_count());

        assert_eq!(
            0,
            db.journal_manager
                .read()
                .expect("lock is poisoned")
                .sealed_journal_count()
        );

        assert_eq!(
            0,
            db.flush_manager
                .read()
                .expect("lock is poisoned")
                .queued_size()
        );

        assert_eq!(0, db.flush_manager.read().expect("lock is poisoned").len());

        for _ in 0..100 {
            tree.insert(nanoid::nanoid!(), "abc")?;
        }

        tree.rotate_memtable()?;

        assert_eq!(1, db.flush_manager.read().expect("lock is poisoned").len());

        assert_eq!(
            1,
            db.journal_manager
                .read()
                .expect("lock is poisoned")
                .sealed_journal_count()
        );

        assert_eq!(0, tree.segment_count());

        db.force_flush()?;

        assert_eq!(
            0,
            db.flush_manager
                .read()
                .expect("lock is poisoned")
                .queued_size()
        );

        assert_eq!(0, db.flush_manager.read().expect("lock is poisoned").len());

        assert_eq!(
            0,
            db.journal_manager
                .read()
                .expect("lock is poisoned")
                .sealed_journal_count()
        );

        assert_eq!(0, db.write_buffer_size());
        assert_eq!(1, tree.segment_count());

        Ok(())
    }
}
