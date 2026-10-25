// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    batch::WriteBatch,
    builder::Openable,
    db_config::Config,
    file::{fsync_directory, CURRENT_VERSION_MARKER, KEYSPACES_FOLDER, LOCK_FILE, VERSION_MARKER},
    flush::manager::FlushManager,
    keyspace::{name::is_valid_keyspace_name, KeyspaceKey},
    locked_file::LockedFileGuard,
    poison::{PoisonDart, PoisonSignal},
    snapshot::Snapshot,
    snapshot_tracker::SnapshotTracker,
    stats::Stats,
    supervisor::{Supervisor, SupervisorInner},
    version::FormatVersion,
    worker_pool::{WorkerMessage, WorkerPool},
    write_buffer_manager::WriteBufferManager,
    write_pipeline::WritePipeline,
    DurabilityLevel, HashMap, Keyspace, KeyspaceCreateOptions,
};
use lsm_tree::SequenceNumberCounter;
use std::{
    fs::remove_dir_all,
    path::Path,
    sync::{atomic::AtomicUsize, Arc, Mutex, RwLock},
};

pub type Keyspaces = HashMap<KeyspaceKey, Keyspace>;

pub struct DatabaseInner {
    // pub(crate) meta_keyspace: MetaKeyspace,
    /// Database configuration
    #[doc(hidden)]
    pub config: Config,

    #[doc(hidden)]
    pub supervisor: Supervisor,

    /// Stop signal when database is dropped to stop background threads
    pub(crate) stop_signal: lsm_tree::stop_signal::StopSignal,

    /// Counter of background threads
    pub(crate) active_thread_counter: Arc<AtomicUsize>,

    /// True if fsync failed
    pub(crate) is_poisoned: PoisonSignal,

    pub(crate) stats: Arc<Stats>,

    pub(crate) keyspace_id_counter: SequenceNumberCounter,

    pub worker_pool: WorkerPool,

    pub(crate) lock_file: LockedFileGuard,
}

impl Drop for DatabaseInner {
    fn drop(&mut self) {
        log::debug!("Dropping database");

        self.stop_signal.send();

        while self
            .active_thread_counter
            .load(std::sync::atomic::Ordering::Relaxed)
            > 0
        {
            let _ = self.worker_pool.rx.drain().count();
            let _ = self.worker_pool.sender.try_send(WorkerMessage::Close);
            std::thread::sleep(std::time::Duration::from_micros(10));
        }

        // Drain again after threads are closed
        let _ = self.worker_pool.rx.drain().count();

        // IMPORTANT: Break cyclic Arcs
        self.supervisor.flush_manager.clear();

        self.supervisor
            .keyspaces
            .write()
            .expect("lock is poisoned")
            .clear();

        /*self.supervisor
        .journal_manager
        .write()
        .expect("lock is poisoned")
        .clear();*/

        /*if self.config.clean_path_on_drop {
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
        }*/

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

impl Openable for Database {
    fn open(config: Config) -> crate::Result<Self>
    where
        Self: Sized,
    {
        Self::open(config)
    }
}

impl Database {
    /// Opens a cross-keyspace snapshot.
    ///
    /// # Caution
    ///
    /// Note that for serializable semantics you need to use a transactional database instead.
    #[must_use]
    pub fn snapshot(&self) -> Snapshot {
        Snapshot::new(self.supervisor.snapshot_tracker.open())
    }

    /// Creates a new database builder to create or open a database at `path`.
    pub fn builder(path: impl AsRef<Path>) -> crate::DatabaseBuilder<Self> {
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
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
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
    pub fn batch(&self) -> WriteBatch {
        let mut batch = WriteBatch::new(self.clone());

        if !self.config.manual_journal_persist {
            batch = batch.durability(Some(DurabilityLevel::Buffer));
        }

        batch
    }

    // TODO: refactor: accessor to stats(), so we don't have that many methods in DB

    /// Returns the current write buffer size (active + sealed memtables).
    ///
    /// # Experimental
    ///
    /// This is a non-stable API currently.
    #[must_use]
    #[doc(hidden)]
    pub fn write_buffer_size(&self) -> u64 {
        self.supervisor.write_buffer_size.get()
    }

    /// Returns the number of queued memtable flush tasks.
    ///
    /// # Experimental
    ///
    /// This is a non-stable API currently.
    #[doc(hidden)]
    #[must_use]
    pub fn outstanding_flushes(&self) -> usize {
        self.supervisor.flush_manager.len()
    }

    /// Returns the time all compactions took until now.
    ///
    /// # Experimental
    ///
    /// This is a non-stable API currently.
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
    ///
    /// # Experimental
    ///
    /// This is a non-stable API currently.
    #[doc(hidden)]
    #[must_use]
    pub fn active_compactions(&self) -> usize {
        self.stats
            .active_compaction_count
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Returns the number of completed compactions.
    ///
    /// # Experimental
    ///
    /// This is a non-stable API currently.
    #[doc(hidden)]
    #[must_use]
    pub fn compactions_completed(&self) -> usize {
        self.stats
            .compactions_completed
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Returns the number of journals on disk.
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
        /*self.supervisor
        .journal_manager
        .read()
        .expect("lock is poisoned")
        .journal_count()*/
        todo!()
    }

    /// Returns the disk space usage of the journal.
    #[doc(hidden)]
    pub fn journal_disk_space(&self) -> crate::Result<u64> {
        /*Ok(self.supervisor.journal.get_writer()?.len()?
        + self
            .supervisor
            .journal_manager
            .read()
            .expect("lock is poisoned")
            .disk_space_used())*/
        todo!()
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
    /// # let _tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
    /// assert!(db.disk_space()? > 0);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    pub fn disk_space(&self) -> crate::Result<u64> {
        let journal_size = self.journal_disk_space()?;

        let keyspaces_size = self
            .supervisor
            .keyspaces
            .read()
            .expect("lock is poisoned")
            .values()
            .map(Keyspace::disk_space)
            .sum::<u64>();

        Ok(journal_size + keyspaces_size)
    }

    /// Flushes the active journal. The durability depends on the [`DurabilityLevel`]
    /// used.
    ///
    /// Persisting only affects durability, NOT consistency! Even without flushing
    /// data is crash-safe.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{DurabilityLevel, Database, KeyspaceCreateOptions};
    /// # let folder = tempfile::tempdir()?;
    /// let db = Database::builder(folder).open()?;
    /// let items = db.keyspace("my_items", KeyspaceCreateOptions::default)?;
    ///
    /// items.insert("a", "hello")?;
    ///
    /// db.persist(DurabilityLevel::SyncAll)?;
    /// #
    /// # Ok::<_, fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Returns error, if an IO error occurred.
    pub fn persist(&self, mode: DurabilityLevel) -> crate::Result<()> {
        if self.is_poisoned.is_poisoned() {
            return Err(crate::Error::Poisoned);
        }

        /*self.supervisor.journal.persist(mode).inspect_err(|e| {
            log::error!(
                "flush failed, which is a FATAL, and possibly hardware-related, failure: {e:?}",
            );
            self.is_poisoned.poison();
        })?;*/

        todo!();

        Ok(())
    }

    #[doc(hidden)]
    #[must_use]
    pub fn cache_capacity(&self) -> u64 {
        self.config.cache.capacity()
    }

    #[doc(hidden)]
    #[must_use]
    pub fn cache_size(&self) -> u64 {
        self.config.cache.size()
    }

    /// Opens a database in the given directory.
    ///
    /// # Errors
    ///
    /// Returns error, if an IO error occurred.
    pub fn open(config: Config) -> crate::Result<Self> {
        log::info!("Opening or creating database at {}", config.path.display());
        log::trace!(
            "Cache capacity={}MiB",
            config.cache.capacity() / 1_024 / 1_024,
        );

        let db = Self::create_or_recover(config)?;
        // db.start_background_threads()?;

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
        let db = if config.path.join(VERSION_MARKER).try_exists()? {
            Self::recover(config)
        } else {
            Self::create_new(config)
        }?;

        /*if db.config.worker_threads > 0 {
            db.worker_pool.start(
                db.config.worker_threads,
                &db.supervisor,
                &db.stats,
                &PoisonDart::new(db.is_poisoned.clone()),
                &db.active_thread_counter,
            )?;
        }
        */

        Ok(db)
    }

    /// Destroys the keyspace, removing all data associated with it.
    ///
    /// The keyspace folder will not be deleted until all references to it are dropped,
    /// so calling this is safe, even if the keyspace is still accessed in another thread.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    #[expect(clippy::needless_pass_by_value)]
    pub fn delete_keyspace(&self, handle: Keyspace) -> crate::Result<()> {
        //self.meta_keyspace.remove_keyspace(&handle.name)?;
        todo!("persist deletion");

        handle
            .is_deleted
            .store(true, std::sync::atomic::Ordering::Release);

        Ok(())
    }

    fn allocate_keyspace_id(&self) -> crate::Result<u64> {
        let mut keyspace_id = self.keyspace_id_counter.next();

        while self
            .config
            .path
            .join(KEYSPACES_FOLDER)
            .join(keyspace_id.to_string())
            .try_exists()?
        {
            keyspace_id = self.keyspace_id_counter.next()
        }

        Ok(keyspace_id)
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
        create_options: impl FnOnce() -> KeyspaceCreateOptions,
    ) -> crate::Result<Keyspace> {
        assert!(is_valid_keyspace_name(name));

        let keyspaces = self.supervisor.keyspaces.write().expect("lock is poisoned");

        Ok(if let Some(keyspace) = keyspaces.get(name) {
            log::trace!("Opening existing keyspace {name:?}");
            keyspace.clone()
        } else {
            let keyspace_id = self.allocate_keyspace_id()?;

            let name: KeyspaceKey = name.into();
            let mut opts = create_options();

            // Install compaction filter factory if needed
            if let Some(f) = self
                .config
                .compaction_filter_factory_assigner
                .as_ref()
                .and_then(|f| f(&name))
            {
                opts = opts.with_compaction_filter_factory(f);
            }

            // TODO: persist keyspace in Manifest

            let handle = Keyspace::create_new(keyspace_id, self, name, opts)?;

            #[cfg(feature = "__internal_whitebox")]
            crate::drop::increment_drop_counter();

            handle
        })
    }

    /// Returns the number of keyspaces.
    #[must_use]
    pub fn keyspace_count(&self) -> usize {
        self.supervisor.keyspace_count()
    }

    /// Gets a list of all keyspace names in the database.
    #[must_use]
    pub fn list_keyspace_names(&self) -> Vec<KeyspaceKey> {
        self.supervisor.list_keyspace_names()
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
    /// db.keyspace("default", KeyspaceCreateOptions::default)?;
    /// assert!(db.keyspace_exists("default"));
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    #[must_use]
    pub fn keyspace_exists(&self, name: &str) -> bool {
        self.supervisor.keyspace_exists(name)
    }

    /// Gets the would-be-next sequence number.
    #[must_use]
    #[doc(hidden)]
    pub fn seqno(&self) -> crate::SeqNo {
        self.supervisor.seqno.get()
    }

    /// Gets the currently visible sequence number.
    #[must_use]
    #[doc(hidden)]
    pub fn visible_seqno(&self) -> crate::SeqNo {
        self.supervisor.snapshot_tracker.get()
    }

    fn check_version<P: AsRef<Path>>(path: P) -> crate::Result<()> {
        let bytes = std::fs::read(path.as_ref().join(VERSION_MARKER))?;

        if let Some(version) = FormatVersion::parse_file_header(&bytes) {
            if version == FormatVersion::V2 {
                log::error!("It looks like you are trying to open a V2 database - the database needs a manual migration, a tool is available at https://github.com/fjall-rs/migrate-v2-v3.");
            }
            if version as u8 > 4 {
                log::error!("It looks like you are trying to open a database from the future. Are you a time traveller?");
            }
            if version != FormatVersion::V3 && version != FormatVersion::V4 {
                return Err(crate::Error::InvalidVersion(Some(version)));
            }
        } else {
            return Err(crate::Error::InvalidVersion(None));
        }

        Ok(())
    }

    /// Returns the database path.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.config.path
    }

    /// Backs up the database into the given destination folder.
    ///
    /// If the destination folder is already an older backup,
    /// it will be incrementally updated.
    ///
    /// If possible, data will be hard-linked.
    ///
    /// # Errors
    ///
    /// Returns error, if an IO error occurred.
    pub fn backup_into<P: AsRef<Path>>(&self, path: P) -> crate::Result<()> {
        let path = path.as_ref();
        log::info!("Backing up database into {}", path.display());
        todo!();
    }

    pub(crate) fn prepare_database(
        config: Config,
        lock_file: LockedFileGuard,
        recover: bool,
    ) -> crate::Result<Self> {
        let keyspaces = Arc::new(RwLock::default());
        let seqno = SequenceNumberCounter::default();
        let visible_seqno = SequenceNumberCounter::default();
        let write_buffer_size = WriteBufferManager::default();

        let wal = if recover {
            // TODO: need to recover keyspaces before

            log::info!("Recovering WAL");

            // TODO: actually do WAL recovery somewhere else
            let mut wal = hwal::Wal::recover(&config.path)?;

            for record in wal.by_ref() {
                let record = record?;

                // TODO: replay into memtables
                let payload = record.payload();
                eprintln!("recovered KV: {payload:?}");

                // TODO: use record.fragment_id() to save watermarks
            }

            wal.into_writer()?
        } else {
            log::info!("Creating v4 WAL");
            hwal::Wal::create(&config.path)?
        };

        let snapshot_tracker = SnapshotTracker::new(visible_seqno);
        let poison_signal = PoisonSignal::default();

        let write_pipeline = WritePipeline::with_capacity(
            crate::write_pipeline::State {
                writer: Mutex::new(crate::wal::Wal::new(wal)),
                seqno: seqno.clone(),
                snapshot_tracker: snapshot_tracker.clone(),
                write_buffer_size: write_buffer_size.clone(),
                poison_flag: poison_signal.clone(),
            },
            config.write_pipeline_leader_capacity,
        );

        let supervisor = Supervisor::new(SupervisorInner {
            db_config: config.clone(),
            keyspaces,
            flush_manager: FlushManager::new(),
            write_buffer_size,
            snapshot_tracker,
            backpressure_lock: Mutex::default(),
            seqno,
            write_pipeline,
        });

        let active_thread_counter = Arc::<AtomicUsize>::default();
        let stats = Arc::<Stats>::default();

        // Construct (empty) database, then fill back with keyspace data
        let inner = DatabaseInner {
            supervisor,
            worker_pool: WorkerPool::prepare(),
            keyspace_id_counter: SequenceNumberCounter::new(1),
            config,
            stop_signal: lsm_tree::stop_signal::StopSignal::default(),
            active_thread_counter,
            is_poisoned: poison_signal,
            stats,
            lock_file,
        };

        Ok(Self(Arc::new(inner)))
    }

    /// Recovers existing database from directory.
    #[doc(hidden)]
    pub fn recover(config: Config) -> crate::Result<Self> {
        log::info!("Recovering database at {}", config.path.display());

        Self::check_version(&config.path)?;

        // We check for the "current" file
        //
        // Once that is written, a previous v4 recovery was definitely fully completed,
        // so if it is missing, we do the v3 migration (again)
        let db = if config.path.join(CURRENT_VERSION_MARKER).try_exists()? {
            log::info!("Recovering v4 database");
            let lock_file = LockedFileGuard::try_acquire(&config.path.join(LOCK_FILE))?;
            Self::prepare_database(config, lock_file, true)
        } else {
            // V3->V4 migration
            //
            // TODO: remove in V5
            let lock_file = LockedFileGuard::try_acquire(&config.path.join(LOCK_FILE))?;
            crate::legacy::migrate_v3_to_v4(config, lock_file)
        }?;

        /*
        db.supervisor
            .snapshot_tracker
            .set(db.supervisor.seqno.get());

        db.supervisor.snapshot_tracker.gc();

        for keyspace in db
            .supervisor
            .keyspaces
            .read()
            .expect("lock is poisoned")
            .values()
        {
            if keyspace.tree.sealed_memtable_count() > 0 {
                log::debug!(
                    "Queuing keyspace {:?} to get flushed because sealed memtables > 0",
                    keyspace.name(),
                );

                // IMPORTANT: Add task to flush manager, so it can be flushed
                db.supervisor
                    .flush_manager
                    .enqueue(Arc::new(crate::flush::Task {
                        keyspace: keyspace.clone(),
                    }));

                keyspace.worker_messager.send(WorkerMessage::Flush).ok();
            } else if keyspace.tree.l0_run_count() > 0 {
                log::debug!(
                    "Queuing keyspace {:?} to maybe get compacted because L0 runs > 0",
                    keyspace.name(),
                );

                keyspace
                    .worker_messager
                    .send(WorkerMessage::Compact(keyspace.clone()))
                    .ok();
            }
        }
        */

        log::trace!("Database recovery successful");

        Ok(db)
    }

    #[doc(hidden)]
    pub fn create_new(config: Config) -> crate::Result<Self> {
        log::info!("Creating database at {}", config.path.display());
        std::fs::create_dir_all(&config.path)?;

        let lock_file = LockedFileGuard::create_new(&config.path.join(LOCK_FILE))?;

        let keyspaces_folder_path = config.path.join(KEYSPACES_FOLDER);
        std::fs::create_dir_all(&keyspaces_folder_path)?;

        let db = Self::prepare_database(config, lock_file, false)?;

        // TODO: v4 create actual db manifest
        std::fs::File::create(db.path().join("current"))?;

        // NOTE: Lastly, fsync version marker, which contains the version
        let mut marker = std::fs::File::create_new(db.path().join(VERSION_MARKER))?;
        FormatVersion::V3.write_file_header(&mut marker)?;
        marker.sync_all()?;

        // IMPORTANT: fsync folders on Unix
        fsync_directory(&keyspaces_folder_path)?;
        fsync_directory(db.path())?;

        Ok(db)
    }
}
