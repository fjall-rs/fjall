// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    batch::WriteBatch,
    builder::Openable,
    db_config::Config,
    file::{
        fsync_directory, rewrite_atomic, BLOBS_FOLDER, LOCK_FILE, TABLES_FOLDER, VERSION_MARKER,
    },
    file_resolver::KeyspaceFileResolver,
    keyspace::{apply_to_base_config, name::is_valid_keyspace_name, KeyspaceKey},
    locked_file::LockedFileGuard,
    manifest::{DbEditRecord, Manifest},
    poison::{PoisonDart, PoisonSignal},
    snapshot::Snapshot,
    snapshot_tracker::SnapshotTracker,
    stats::Stats,
    supervisor::{Supervisor, SupervisorInner},
    version::FormatVersion,
    wal::recover_wal,
    worker_pool::{WorkerMessage, WorkerPool},
    write_buffer_manager::WriteBufferManager,
    write_pipeline::WritePipeline,
    DurabilityLevel, HashMap, Keyspace, KeyspaceCreateOptions,
};
use lsm_tree::SequenceNumberCounter;
use std::{
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

    /// Table ID counter, shared by all trees (tables live in one DB-level folder)
    pub(crate) table_id_counter: SequenceNumberCounter,

    /// Blob file ID counter, shared by all trees (blob files live in one DB-level folder)
    pub(crate) blob_file_id_counter: SequenceNumberCounter,

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

        // IMPORTANT: Drain again after threads are closed,
        // which also breaks cyclic Arcs (messages hold keyspace handles)
        let _ = self.worker_pool.rx.drain().count();

        self.supervisor
            .keyspaces
            .write()
            .expect("lock is poisoned")
            .clear();

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
        WriteBatch::new(self.clone())
    }

    // TODO: v4 refactor: accessor to stats(), so we don't have that many methods in DB

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
    #[expect(clippy::expect_used, clippy::missing_panics_doc)]
    pub fn wal_count(&self) -> usize {
        // 1 = active WAL fragment
        1 + self
            .supervisor
            .ledger()
            .expect("should not be poisoned")
            .wal
            .sealed_fragment_count()
    }

    /// Returns the disk space usage of the write-ahead log.
    #[doc(hidden)]
    pub fn wal_disk_space(&self) -> crate::Result<u64> {
        self.supervisor.ledger()?.wal.disk_usage()
    }

    /// Returns the disk space usage of the database manifest.
    #[doc(hidden)]
    pub fn db_manifest_disk_space(&self) -> crate::Result<u64> {
        self.supervisor.ledger()?.manifest.disk_usage()
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
    ///
    /// # Errors
    ///
    /// Returns error, if an IO error occurred.
    #[expect(clippy::expect_used, clippy::missing_panics_doc)]
    pub fn disk_space(&self) -> crate::Result<u64> {
        let journal_size = self.wal_disk_space()?;

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
        use crate::write_pipeline::WriteRecordPayload;

        let write_record =
            crate::write_pipeline::WriteRecord::new(WriteRecordPayload::Multiple(vec![]))
                .durability(mode);

        #[expect(
            clippy::expect_used,
            reason = "does not use OCC, so should not conflict"
        )]
        #[expect(clippy::missing_panics_doc)]
        self.supervisor
            .write_pipeline
            .commit(Arc::new(write_record))?
            .expect("should not do OCC");

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

        if db.config.worker_threads > 0 {
            db.worker_pool.start(
                db.config.worker_threads,
                &db.supervisor,
                &db.stats,
                &PoisonDart::new(db.is_poisoned.clone()),
                &db.active_thread_counter,
            )?;
        }

        Ok(db)
    }

    /// Destroys the keyspace, removing all data associated with it.
    ///
    /// The keyspace's files will not be deleted until all references to them are dropped
    /// (e.g. snapshots, iterators), so calling this is safe, even if the keyspace is still
    /// accessed in another thread. Writes to a deleted keyspace return [`Error::KeyspaceDeleted`].
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    #[expect(clippy::needless_pass_by_value)]
    pub fn delete_keyspace(&self, handle: Keyspace) -> crate::Result<()> {
        // IMPORTANT: Exclusive, so no compaction (or ingestion) of the keyspace installs
        // its result after the deletion
        let _compaction_lock = handle
            .compaction_lock
            .write()
            .map_err(|_| crate::Error::Poisoned)?;

        let mut ledger = self.supervisor.ledger()?;

        #[expect(clippy::expect_used)]
        let mut keyspaces = self.supervisor.keyspaces.write().expect("lock is poisoned");

        if self.is_poisoned.is_poisoned() {
            return Err(crate::Error::Poisoned);
        }

        if handle.is_deleted.load(std::sync::atomic::Ordering::Acquire) {
            return Err(crate::Error::KeyspaceDeleted);
        }

        log::debug!("Deleting keyspace {:?} (#{})", handle.name, handle.id);

        // IMPORTANT: Persist the deletion first
        //
        // NOTE: A crash after this leaves the keyspace's files behind (unreferenced),
        // but the keyspace stays deleted
        ledger
            .manifest
            .append_db_edits(&[DbEditRecord::DeleteKeyspace { id: handle.id }])?;

        // NOTE: Drops all tables and blob files (they are removed once nothing references them),
        // the edit is ignored on recovery, because the keyspace is deleted
        let seqno = self.supervisor.seqno.next();

        let freed_bytes = handle
            .version_set_provider
            .clear(&mut ledger.persister(handle.id), &handle.tree, seqno)
            .inspect_err(|e| {
                log::error!("Deleting keyspace failed, which is a FATAL failure: {e:?}");
                self.is_poisoned.poison();
            })?;

        handle
            .is_deleted
            .store(true, std::sync::atomic::Ordering::Release);

        keyspaces.remove(&handle.name);

        self.supervisor.write_buffer_size.free(freed_bytes);
        self.supervisor.snapshot_tracker.publish(seqno);

        // NOTE: Old versions (and their files) go away as soon as no snapshot needs them
        handle.prune_versions();

        Ok(())
    }

    /// Allocates a new keyspace ID.
    ///
    /// The counter is recovered from the DB manifest (highest keyspace ID + 1),
    /// and keyspaces no longer have their own folder, so the ID is always free.
    fn allocate_keyspace_id(&self) -> u64 {
        self.keyspace_id_counter.next()
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

        let mut ledger = self.supervisor.ledger()?;

        // IMPORTANT: Check the poisoned flag after getting the ledger lock, otherwise TOCTOU
        if self.is_poisoned.is_poisoned() {
            return Err(crate::Error::Poisoned);
        }

        let mut keyspaces = self.supervisor.keyspaces.write().expect("lock is poisoned");

        Ok(if let Some(keyspace) = keyspaces.get(name) {
            log::trace!("Opening existing keyspace {name:?}");
            keyspace.clone()
        } else {
            let keyspace_id = self.allocate_keyspace_id();

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

            let config = opts.encode_kvs(keyspace_id);
            let handle = Keyspace::create_new(keyspace_id, self, name.clone(), opts)?;

            let mut db_edits = Vec::with_capacity(config.len() + 1);
            db_edits.push(DbEditRecord::CreateKeyspace {
                id: keyspace_id,
                name: name.clone(),
            });
            db_edits.extend(
                config
                    .into_iter()
                    .map(|(k, v)| DbEditRecord::ConfigurationChanged {
                        id: keyspace_id,
                        k,
                        v,
                    }),
            );

            ledger.manifest.append_db_edits(&db_edits)?;

            keyspaces.insert(name, handle.clone());

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
    pub fn next_seqno(&self) -> crate::SeqNo {
        self.supervisor.seqno.get()
    }

    /// Gets the currently visible sequence number.
    ///
    /// As per lsm-tree rules, a visible seqno of 1 means, that items with seqno < 1 are readable.
    #[must_use]
    #[doc(hidden)]
    pub fn visible_seqno(&self) -> crate::SeqNo {
        self.supervisor.snapshot_tracker.get()
    }

    fn check_version<P: AsRef<Path>>(path: P) -> crate::Result<FormatVersion> {
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

            Ok(version)
        } else {
            Err(crate::Error::InvalidVersion(None))
        }
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
    /// Returns an error, if an IO error occurred.
    pub fn backup_into<P: AsRef<Path>>(&self, path: P) -> crate::Result<()> {
        let path = path.as_ref();
        log::info!("Backing up database into {}", path.display());
        todo!();
    }

    /// Synchronously flushes all keyspaces into tables files and rotates the write-ahead log.
    ///
    /// This blocks writes and keyspace changes for the duration of the operation.
    /// Readers may continue.
    ///
    /// The database remains usable afterwards, and reopening it
    /// does not need to replay writes preceding the flush.
    ///
    /// # Errors
    ///
    /// Returns an error if an I/O error occurs.
    pub fn flush_all(&self) -> crate::Result<()> {
        use lsm_tree::AbstractTree;

        log::info!("Flushing all write buffers");

        // NOTE: Lock order: ledger lock, keyspaces lock, flush lock
        let mut ledger = self.supervisor.ledger()?;

        // IMPORTANT: Check the poisoned flag after getting the ledger lock, otherwise TOCTOU
        if self.is_poisoned.is_poisoned() {
            return Err(crate::Error::Poisoned);
        }

        {
            let keyspaces = self
                .supervisor
                .keyspaces
                .read()
                .map_err(|_| crate::Error::Poisoned)?;

            // NOTE: Seal the active WAL fragment first: every item in it is flushed below,
            // so all sealed fragments can be pruned afterwards
            log::debug!("Rotating WAL");
            ledger.wal.rotate(&keyspaces)?;

            for keyspace in keyspaces.values() {
                let _flush_lock = keyspace.tree.get_flush_lock();
                keyspace.flush_locked(&mut ledger)?;
            }
        }

        // NOTE: After the keyspaces lock is released again
        self.supervisor
            .maybe_rotate_manifest(&mut ledger, &self.is_poisoned)?;

        self.supervisor.prune_wal(&mut ledger)
    }

    /// Seals the active WAL fragment, and continues in a new one.
    ///
    /// Only meant for testing.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    #[doc(hidden)]
    pub fn rotate_wal(&self) -> crate::Result<()> {
        let mut ledger = self.supervisor.ledger()?;

        let keyspaces = self
            .supervisor
            .keyspaces
            .read()
            .map_err(|_| crate::Error::Poisoned)?;

        ledger.wal.rotate(&keyspaces)
    }

    /// Rotates the DB manifest: writes a snapshot of the full DB state into a new
    /// manifest fragment, and removes the previous fragment.
    ///
    /// Happens automatically once the manifest is large enough; only meant for testing.
    ///
    /// # Errors
    ///
    /// Returns an error if an I/O error occurs.
    #[doc(hidden)]
    pub fn rotate_manifest(&self) -> crate::Result<()> {
        if self.is_poisoned.is_poisoned() {
            return Err(crate::Error::Poisoned);
        }

        let mut ledger = self.supervisor.ledger()?;

        self.supervisor
            .rotate_manifest(&mut ledger, &self.is_poisoned)
    }

    /// Constructs an ("empty") database to fill back with actual data later on.
    pub(crate) fn prepare_database(
        config: Config,
        lock_file: LockedFileGuard,
        recover: bool,
    ) -> crate::Result<Self> {
        let keyspaces = Arc::new(RwLock::default());
        let seqno = SequenceNumberCounter::default();
        let visible_seqno = SequenceNumberCounter::default();
        let write_buffer_size = WriteBufferManager::default();

        let manifest = if recover {
            Manifest::open(&config.path)?
        } else {
            Manifest::create_new(&config.path)?
        };

        let wal = if recover {
            log::info!("Opening WAL at {}", config.path.display());
            hwal::Wal::builder().open(&config.path)?
        } else {
            log::info!("Creating WAL at {}", config.path.display());
            hwal::Wal::builder().create_new(&config.path)?
        };

        let snapshot_tracker = SnapshotTracker::new(visible_seqno);
        let poison_signal = PoisonSignal::default();

        let ledger = Arc::new(Mutex::new(crate::ledger::Ledger::new(
            crate::wal::Wal::new(wal).with_compression(
                config.wal_compression_type,
                config.wal_compression_threshold,
            ),
            manifest,
        )));

        let write_pipeline = WritePipeline::with_capacity(
            crate::write_pipeline::State {
                ledger: ledger.clone(),
                seqno: seqno.clone(),
                snapshot_tracker: snapshot_tracker.clone(),
                write_buffer_size: write_buffer_size.clone(),
                poison_flag: poison_signal.clone(),
            },
            config.write_pipeline_leader_capacity,
        );

        // NOTE: Shared with the supervisor, which writes it into manifest snapshots
        let keyspace_id_counter = SequenceNumberCounter::new(0);

        let supervisor = Supervisor::new(SupervisorInner {
            db_config: config.clone(),
            keyspace_id_counter: keyspace_id_counter.clone(),
            keyspaces,
            write_buffer_size,
            snapshot_tracker,
            seqno,
            ledger,
            write_pipeline,
            legacy_watermark: std::sync::OnceLock::new(),
        });

        let active_thread_counter = Arc::<AtomicUsize>::default();
        let stats = Arc::default();

        let inner = DatabaseInner {
            supervisor,
            worker_pool: WorkerPool::prepare(),
            keyspace_id_counter,
            table_id_counter: SequenceNumberCounter::default(),
            blob_file_id_counter: SequenceNumberCounter::default(),
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

        let version = Self::check_version(&config.path)?;

        // We check for the "version" file (which holds the format version)
        //
        // Once that is updated, a previous v4 recovery was definitely fully completed,
        // so if it is = 3, we do the v3 migration again
        let db = match version {
            FormatVersion::V4 => {
                log::info!("Recovering v4 database");
                let lock_file: LockedFileGuard =
                    LockedFileGuard::try_acquire(&config.path.join(LOCK_FILE))?;

                let db = Self::prepare_database(config, lock_file, true)?;

                // Look for old v3 data
                {
                    let path = db.path().join("keyspaces");
                    if path.try_exists()? {
                        log::trace!("Removing old v3 keyspaces folder");
                        std::fs::remove_dir_all(path)?;
                    }
                    crate::legacy::v3::journal::nuke_journal(db.path())?;
                }

                log::debug!("Loading keyspace info from DB manifest");
                let manifest_recovery_result = db.supervisor.ledger()?.manifest.recover()?;
                let keyspace_info = &manifest_recovery_result.keyspace_info;
                let keyspace_versions = &manifest_recovery_result.keyspace_versions;

                // NOTE: Kept, so manifest rotations write it into the snapshot again
                if let Some(legacy_watermark) = manifest_recovery_result.legacy_watermark {
                    db.supervisor.legacy_watermark.set(legacy_watermark).ok();
                }

                // NOTE: We set the seqno (that is, the next seqno a write batch would get)
                // to be one higher than the last found seqno (trivial)
                db.supervisor
                    .seqno
                    .fetch_max(manifest_recovery_result.highest_seqno + 1);

                // NOTE: We PUBLISH the last actually found seqno
                db.supervisor
                    .snapshot_tracker
                    .publish(manifest_recovery_result.highest_seqno);

                log::warn!("Got keyspace info: {keyspace_info:#?}");

                // TODO: use DB path for tree path, once v4 folder migration is done;
                // make sure to also set path correctly when creating keyspaces
                // TODO: and create folders on DB::create_new

                let mut registry = db
                    .supervisor
                    .keyspaces
                    .write()
                    .expect("lock should not be poisoned");

                let mut highest_keyspace_id = 0;
                {
                    log::debug!("Loading {} existing keyspaces", keyspace_info.len());

                    for keyspace in keyspace_info.values() {
                        let version_set_provider = Arc::new(lsm_tree::IoVersionSetProvider::new());

                        // NOTE: All trees share the DB-level tables & blobs folders,
                        // same as when creating a keyspace
                        // NOTE: Migrated v3 files are named differently, see `LegacyWatermark`
                        let file_resolver = Arc::new(KeyspaceFileResolver {
                            db_path: db.path().to_path_buf(),
                            keyspace_id: keyspace.id,
                            legacy: manifest_recovery_result.legacy_watermark,
                        });

                        let base_config =
                            lsm_tree::Config::new(db.path(), version_set_provider.clone())
                                .use_descriptor_table(db.config.descriptor_table.clone())
                                .use_cache(db.config.cache.clone())
                                .with_table_id_counter(db.table_id_counter.clone())
                                .with_blob_file_id_counter(db.blob_file_id_counter.clone())
                                .with_file_resolver(file_resolver);

                        let mut recovered_config = KeyspaceCreateOptions::from_kvs(&keyspace)?;

                        // Install compaction filter factory if needed
                        if let Some(f) = db
                            .config
                            .compaction_filter_factory_assigner
                            .as_ref()
                            .and_then(|f| f(&keyspace.name))
                        {
                            recovered_config = recovered_config.with_compaction_filter_factory(f);
                        }

                        let base_config = apply_to_base_config(base_config, &recovered_config);

                        let tree = base_config.open()?;

                        let keyspace = Keyspace::from_database(
                            keyspace.id,
                            &db,
                            tree,
                            keyspace.name.clone(),
                            recovered_config,
                            version_set_provider,
                        );

                        // Add keyspace to dictionary
                        registry.insert(keyspace.name.clone(), keyspace.clone());

                        highest_keyspace_id = highest_keyspace_id.max(keyspace.id);
                    }

                    log::debug!("Recovered {} keyspaces", registry.len());

                    // IMPORTANT: Also above the IDs of deleted keyspaces, so no ID is ever reused
                    db.keyspace_id_counter.set(
                        (highest_keyspace_id + 1).max(manifest_recovery_result.next_keyspace_id),
                    );
                    log::trace!(
                        "Keyspace ID counter set to {}",
                        db.keyspace_id_counter.get(),
                    );
                }

                log::debug!("Replaying DB manifest");
                {
                    for (&kid, vdata) in keyspace_versions {
                        if let Some(keyspace) = registry.values().find(|ks| ks.id() == kid) {
                            keyspace
                                .version_set_provider
                                .hydrate(&keyspace.tree, vdata)?;
                        }
                    }
                }

                // IMPORTANT: Continue after every item in the keyspaces' tables (and cleared items):
                // the flush watermark is at or above all of them. The highest `SetVersionSeqno`
                // above does not cover migrated v3 trees, whose versions have no seqno; their
                // flush watermark falls back to their highest table seqno.
                //
                // Otherwise:
                // 1. v3: insert, flush (so the journal has nothing left to replay)
                // 2. migrate, then reopen in v4: the seqno starts at 0 again,
                //    so the migrated data is invisible
                {
                    use lsm_tree::AbstractTree;

                    for keyspace in registry.values() {
                        if let Some(flush_watermark) = keyspace.tree.get_flush_watermark() {
                            db.supervisor.seqno.fetch_max(flush_watermark + 1);
                            db.supervisor.snapshot_tracker.publish(flush_watermark);
                        }
                    }
                }

                log::debug!("Replaying WAL");
                {
                    // TODO: change to be Option<Seqno>
                    // NOTE: Unknown IDs belong to deleted keyspaces
                    let highest_seqno = recover_wal(&db, &registry, |id| {
                        registry
                            .values()
                            .find(|ks| ks.id() == id)
                            .map(|ks| ks.name())
                            .cloned()
                    })?;

                    // See notes above
                    db.supervisor.seqno.fetch_max(highest_seqno + 1);
                    db.supervisor.snapshot_tracker.publish(highest_seqno);
                }

                drop(registry);

                Ok(db)
            }
            FormatVersion::V3 => {
                // V3->V4 migration
                //
                // TODO: remove in V5
                let lock_file = LockedFileGuard::try_acquire(&config.path.join(LOCK_FILE))?;
                crate::legacy::migrate_v3_to_v4(config, lock_file)
            }
            _ => unreachable!("should already be checked"),
        }?;

        for keyspace in db
            .supervisor
            .keyspaces
            .read()
            .expect("lock is poisoned")
            .values()
        {
            use lsm_tree::AbstractTree;

            if keyspace.tree.sealed_memtable_count() > 0 {
                log::debug!(
                    "Queuing keyspace {:?} to get flushed because sealed memtables > 0",
                    keyspace.name(),
                );

                keyspace
                    .worker_messager
                    .send(WorkerMessage::Flush(keyspace.clone()))
                    .ok();
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

        log::trace!("Database recovery successful");

        Ok(db)
    }

    #[doc(hidden)]
    pub fn create_new(config: Config) -> crate::Result<Self> {
        log::info!("Creating database at {}", config.path.display());
        std::fs::create_dir_all(&config.path)?;

        let lock_file = LockedFileGuard::create_new(&config.path.join(LOCK_FILE))?;

        let tables_folder_path = config.path.join(TABLES_FOLDER);
        let blobs_folder_path = config.path.join(BLOBS_FOLDER);

        std::fs::create_dir_all(&tables_folder_path)?;
        std::fs::create_dir_all(&blobs_folder_path)?;

        let db = Self::prepare_database(config, lock_file, false)?;

        // IMPORTANT: Set the "current" marker (written atomically)
        db.supervisor.ledger()?.manifest.place_current_marker()?;

        // NOTE: Lastly, fsync version marker, which contains the version
        {
            let mut v = vec![];
            crate::version::FormatVersion::V4.write_file_header(&mut v)?;
            rewrite_atomic(&db.path().join(VERSION_MARKER), &v)?;
        }

        // IMPORTANT: fsync folders on Unix
        fsync_directory(&tables_folder_path)?;
        fsync_directory(&blobs_folder_path)?;
        fsync_directory(db.path())?;

        Ok(db)
    }
}
