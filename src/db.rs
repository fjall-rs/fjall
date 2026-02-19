// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    batch::WriteBatch,
    db_config::Config,
    file::{fsync_directory, KEYSPACES_FOLDER, LOCK_FILE, VERSION_MARKER},
    flush::manager::FlushManager,
    journal::{manager::JournalManager, writer::PersistMode, Journal},
    keyspace::{name::is_valid_keyspace_name, KeyspaceKey},
    locked_file::LockedFileGuard,
    meta_keyspace::MetaKeyspace,
    poison_dart::PoisonDart,
    recovery::{recover_keyspaces, recover_sealed_memtables},
    snapshot::Snapshot,
    snapshot_tracker::SnapshotTracker,
    stats::Stats,
    supervisor::{Supervisor, SupervisorInner},
    tx::single_writer::Openable,
    version::FormatVersion,
    worker_pool::{WorkerMessage, WorkerPool},
    write_buffer_manager::WriteBufferManager,
    HashMap, Keyspace, KeyspaceCreateOptions,
};
use lsm_tree::{AbstractTree, SequenceNumberCounter};
use std::{
    fs::remove_dir_all,
    path::Path,
    sync::{
        atomic::{AtomicBool, AtomicUsize},
        Arc, Mutex, RwLock,
    },
};

pub type Keyspaces = HashMap<KeyspaceKey, Keyspace>;

pub struct DatabaseInner {
    pub(crate) meta_keyspace: MetaKeyspace,

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
    pub(crate) is_poisoned: Arc<AtomicBool>,

    pub(crate) stats: Arc<Stats>,

    pub(crate) keyspace_id_counter: SequenceNumberCounter,

    pub worker_pool: WorkerPool,

    pub(crate) lock_file: LockedFileGuard,
}

impl Drop for DatabaseInner {
    fn drop(&mut self) {
        log::debug!("Dropping database");

        self.stop_signal.send();

        let _ = self.worker_pool.rx.drain().count();

        while self
            .active_thread_counter
            .load(std::sync::atomic::Ordering::Relaxed)
            > 0
        {
            let _ = self.worker_pool.sender.send(WorkerMessage::Close);
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
        self.supervisor
            .journal_manager
            .write()
            .expect("lock is poisoned")
            .clear();

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
            batch = batch.durability(Some(PersistMode::Buffer));
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

    /// Returns the amount of completed compactions.
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
        self.supervisor
            .journal_manager
            .read()
            .expect("lock is poisoned")
            .journal_count()
    }

    /// Returns the disk space usage of the journal.
    #[doc(hidden)]
    pub fn journal_disk_space(&self) -> crate::Result<u64> {
        Ok(self.supervisor.journal.get_writer().len()?
            + self
                .supervisor
                .journal_manager
                .read()
                .expect("lock is poisoned")
                .disk_space_used())
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
    /// let items = db.keyspace("my_items", KeyspaceCreateOptions::default)?;
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

        if let Err(e) = self.supervisor.journal.persist(mode) {
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
        if config.path.join(VERSION_MARKER).try_exists()? {
            Self::recover(config)
        } else {
            Self::create_new(config)
        }
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
        self.meta_keyspace.remove_keyspace(&handle.name)?;

        handle
            .is_deleted
            .store(true, std::sync::atomic::Ordering::Release);

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
        create_options: impl FnOnce() -> KeyspaceCreateOptions,
    ) -> crate::Result<Keyspace> {
        assert!(is_valid_keyspace_name(name));

        let keyspaces = self.supervisor.keyspaces.write().expect("lock is poisoned");

        Ok(if let Some(keyspace) = keyspaces.get(name) {
            keyspace.clone()
        } else {
            let name: KeyspaceKey = name.into();

            let keyspace_id = self.keyspace_id_counter.next();

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

            let handle = Keyspace::create_new(keyspace_id, self, name.clone(), opts)?;

            self.meta_keyspace
                .create_keyspace(keyspace_id, &name, handle.clone(), keyspaces)?;

            #[cfg(feature = "__internal_whitebox")]
            crate::drop::increment_drop_counter();

            handle
        })
    }

    /// Returns the number of keyspaces.
    #[must_use]
    pub fn keyspace_count(&self) -> usize {
        self.supervisor
            .keyspaces
            .read()
            .expect("lock is poisoned")
            .len()
    }

    /// Gets a list of all keyspace names in the database.
    #[must_use]
    pub fn list_keyspace_names(&self) -> Vec<KeyspaceKey> {
        self.supervisor
            .keyspaces
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
    /// db.keyspace("default", KeyspaceCreateOptions::default)?;
    /// assert!(db.keyspace_exists("default"));
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    #[must_use]
    pub fn keyspace_exists(&self, name: &str) -> bool {
        self.meta_keyspace.keyspace_exists(name)
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
            if version as u8 > 3 {
                log::error!("It looks like you are trying to open a database from the future. Are you a time traveller?");
            }
            if version != FormatVersion::V3 {
                return Err(crate::Error::InvalidVersion(Some(version)));
            }
        } else {
            return Err(crate::Error::InvalidVersion(None));
        }

        Ok(())
    }

    /// Recovers existing database from directory.
    #[expect(clippy::too_many_lines)]
    #[doc(hidden)]
    pub fn recover(config: Config) -> crate::Result<Self> {
        log::info!("Recovering database at {}", config.path.display());

        // Check version
        Self::check_version(&config.path)?;

        let lock_file = LockedFileGuard::try_acquire(&config.path.join(LOCK_FILE))?;

        // TODO:
        // let recovery_mode = config.journal_recovery_mode;

        // Reload active journal
        let journal_recovery = Journal::recover(
            &config.path,
            config.journal_compression_type,
            config.journal_compression_threshold,
        )?;
        log::debug!("journal recovery result: {journal_recovery:#?}");

        let active_journal = Arc::new(journal_recovery.active);
        active_journal.get_writer().persist(PersistMode::SyncAll)?;

        let sealed_journals = journal_recovery.sealed;

        let journal_manager = JournalManager::new();

        let seqno = SequenceNumberCounter::default();
        let visible_seqno = SequenceNumberCounter::default();

        let keyspaces = Arc::new(RwLock::new(Keyspaces::with_capacity_and_hasher(
            10,
            xxhash_rust::xxh3::Xxh3Builder::new(),
        )));

        let meta_tree = lsm_tree::Config::new(
            config.path.join(KEYSPACES_FOLDER).join("0"),
            seqno.clone(),
            visible_seqno.clone(),
        )
        .use_cache(config.cache.clone())
        .use_descriptor_table(config.descriptor_table.clone())
        .expect_point_read_hits(true)
        .data_block_size_policy(crate::config::BlockSizePolicy::all(4_096))
        .data_block_hash_ratio_policy(crate::config::HashRatioPolicy::all(8.0))
        .data_block_compression_policy(crate::config::CompressionPolicy::disabled())
        .data_block_restart_interval_policy(crate::config::RestartIntervalPolicy::all(1))
        .index_block_compression_policy(crate::config::CompressionPolicy::disabled())
        .filter_policy(crate::config::FilterPolicy::new([
            lsm_tree::config::FilterPolicyEntry::Bloom(
                lsm_tree::config::BloomConstructionPolicy::FalsePositiveRate(0.0001),
            ),
            lsm_tree::config::FilterPolicyEntry::Bloom(
                lsm_tree::config::BloomConstructionPolicy::FalsePositiveRate(0.01),
            ),
        ]))
        .open()?;

        let meta_keyspace = MetaKeyspace::new(
            meta_tree,
            keyspaces.clone(),
            seqno.clone(),
            visible_seqno.clone(),
        );

        let supervisor = Supervisor::new(SupervisorInner {
            db_config: config.clone(),
            keyspaces: keyspaces.clone(),
            flush_manager: FlushManager::new(),
            write_buffer_size: WriteBufferManager::default(),
            snapshot_tracker: SnapshotTracker::new(visible_seqno),
            journal: active_journal.clone(),
            journal_manager: Arc::new(RwLock::new(journal_manager)),
            backpressure_lock: Mutex::default(),
            seqno,
        });

        let active_thread_counter = Arc::<AtomicUsize>::default();
        let stats = Arc::<Stats>::default();

        let is_poisoned = Arc::<AtomicBool>::default();

        // Construct (empty) database, then fill back with keyspace data
        let inner = DatabaseInner {
            supervisor,
            worker_pool: WorkerPool::prepare(),
            keyspace_id_counter: SequenceNumberCounter::new(1),
            meta_keyspace: meta_keyspace.clone(),
            config,
            stop_signal: lsm_tree::stop_signal::StopSignal::default(),
            active_thread_counter,
            is_poisoned,
            stats,
            lock_file,
        };

        let db = Self(Arc::new(inner));

        // Recover keyspaces
        recover_keyspaces(&db, &meta_keyspace)?;

        // Recover sealed memtables by walking through old journals
        recover_sealed_memtables(
            &db,
            &sealed_journals
                .into_iter()
                .map(|(_, x)| x)
                .collect::<Vec<_>>(),
        )?;

        {
            #[expect(clippy::expect_used)]
            let keyspaces = db.supervisor.keyspaces.read().expect("lock is poisoned");

            // NOTE: If this triggers, the last sealed memtable
            // was not correctly rotated
            for keyspace in keyspaces.values() {
                if keyspace.tree.active_memtable().size() > 0 {
                    log::error!("Active memtable is not empty after recovery for keyspace {:?} - recovery failed", keyspace.name);
                    return Err(crate::Error::Unrecoverable);
                }
            }

            // NOTE: We only need to recover the active journal, if it actually existed before
            // nothing to recover, if we just created it
            if !journal_recovery.was_active_created {
                log::trace!("Recovering active memtables from active journal");

                let reader = db.supervisor.journal.get_reader()?;

                for batch in reader {
                    let batch = batch?;

                    for item in batch.items {
                        let Some(keyspace_name) = db.meta_keyspace.resolve_id(item.keyspace_id)?
                        else {
                            continue;
                        };

                        let Some(keyspace) = keyspaces.get(&keyspace_name) else {
                            continue;
                        };

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
                            lsm_tree::ValueType::Indirection => {
                                unreachable!()
                            }
                        }
                    }

                    for keyspace_id in &batch.cleared_keyspaces {
                        let Some(keyspace_name) = db.meta_keyspace.resolve_id(*keyspace_id)? else {
                            continue;
                        };

                        let Some(keyspace) = keyspaces.get(&keyspace_name) else {
                            continue;
                        };

                        keyspace.tree.clear().ok();
                    }
                }

                for keyspace in keyspaces.values() {
                    let size = keyspace.tree.active_memtable().size();

                    log::trace!(
                        "Recovered active memtable of size {size}B for keyspace {:?} ({} items)",
                        keyspace.name,
                        keyspace.tree.active_memtable().len(),
                    );

                    // IMPORTANT: Add active memtable size to current write buffer size
                    db.supervisor.write_buffer_size.allocate(size);

                    // Recover seqno
                    let maybe_next_seqno = keyspace
                        .tree
                        .get_highest_seqno()
                        .map(|x| x + 1)
                        .unwrap_or_default();

                    db.supervisor.seqno.fetch_max(maybe_next_seqno);
                    log::debug!("Database seqno is now {}", db.supervisor.seqno.get());
                }
            }
        }

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
                keyspace.worker_messager.send(WorkerMessage::Flush).ok();
            }
        }

        db.worker_pool.start(
            db.config.worker_threads,
            &db.supervisor,
            &db.stats,
            &PoisonDart::new(db.is_poisoned.clone()),
            &db.active_thread_counter,
        )?;

        log::trace!("Recovery successful");

        Ok(db)
    }

    #[doc(hidden)]
    pub fn create_new(config: Config) -> crate::Result<Self> {
        log::info!("Creating database at {}", config.path.display());

        std::fs::create_dir_all(&config.path)?;

        let lock_file = LockedFileGuard::create_new(&config.path.join(LOCK_FILE))?;

        let journal_folder_path = &config.path;
        let keyspaces_folder_path = config.path.join(KEYSPACES_FOLDER);

        std::fs::create_dir_all(&keyspaces_folder_path)?;

        let active_journal_path = journal_folder_path.join("0.jnl");
        let journal = Journal::create_new(&active_journal_path)?.with_compression(
            config.journal_compression_type,
            config.journal_compression_threshold,
        );
        let journal = Arc::new(journal);

        // NOTE: Lastly, fsync version marker, which contains the version
        let mut marker = std::fs::File::create_new(config.path.join(VERSION_MARKER))?;
        FormatVersion::V3.write_file_header(&mut marker)?;
        marker.sync_all()?;

        // IMPORTANT: fsync folders on Unix
        fsync_directory(&keyspaces_folder_path)?;
        fsync_directory(&config.path)?;

        let seqno = SequenceNumberCounter::default();
        let visible_seqno = SequenceNumberCounter::default();

        let keyspaces = Arc::new(RwLock::new(Keyspaces::with_capacity_and_hasher(
            10,
            xxhash_rust::xxh3::Xxh3Builder::new(),
        )));

        let meta_tree = lsm_tree::Config::new(
            config.path.join(KEYSPACES_FOLDER).join("0"),
            seqno.clone(),
            visible_seqno.clone(),
        )
        .use_cache(config.cache.clone())
        .use_descriptor_table(config.descriptor_table.clone())
        .expect_point_read_hits(true)
        .data_block_size_policy(crate::config::BlockSizePolicy::all(4_096))
        .data_block_hash_ratio_policy(crate::config::HashRatioPolicy::all(8.0))
        .data_block_compression_policy(crate::config::CompressionPolicy::disabled())
        .data_block_restart_interval_policy(crate::config::RestartIntervalPolicy::all(1))
        .index_block_compression_policy(crate::config::CompressionPolicy::disabled())
        .filter_policy(crate::config::FilterPolicy::new([
            lsm_tree::config::FilterPolicyEntry::Bloom(
                lsm_tree::config::BloomConstructionPolicy::FalsePositiveRate(0.0001),
            ),
            lsm_tree::config::FilterPolicyEntry::Bloom(
                lsm_tree::config::BloomConstructionPolicy::FalsePositiveRate(0.01),
            ),
        ]))
        .open()?;

        let meta_keyspace = MetaKeyspace::new(
            meta_tree,
            keyspaces.clone(),
            seqno.clone(),
            visible_seqno.clone(),
        );

        let supervisor = Supervisor::new(SupervisorInner {
            db_config: config.clone(),
            keyspaces,
            flush_manager: FlushManager::new(),
            write_buffer_size: WriteBufferManager::default(),
            snapshot_tracker: SnapshotTracker::new(visible_seqno),
            journal,
            journal_manager: Arc::new(RwLock::new(JournalManager::new())),
            backpressure_lock: Mutex::default(),
            seqno,
        });

        let active_thread_counter = Arc::<AtomicUsize>::default();
        let stats = Arc::<Stats>::default();

        let is_poisoned = Arc::<AtomicBool>::default();

        let inner = DatabaseInner {
            supervisor,
            worker_pool: WorkerPool::prepare(),
            keyspace_id_counter: SequenceNumberCounter::new(1),
            meta_keyspace,
            config,
            stop_signal: lsm_tree::stop_signal::StopSignal::default(),
            active_thread_counter,
            is_poisoned,
            stats,
            lock_file,
        };

        let db = Self(Arc::new(inner));

        db.worker_pool.start(
            db.config.worker_threads,
            &db.supervisor,
            &db.stats,
            &PoisonDart::new(db.is_poisoned.clone()),
            &db.active_thread_counter,
        )?;

        Ok(db)
    }
}
