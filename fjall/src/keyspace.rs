use crate::{
    compaction::manager::CompactionManager,
    config::Config,
    file::{
        FJALL_MARKER, FLUSH_MARKER, FLUSH_PARTITIONS_LIST, JOURNALS_FOLDER, PARTITIONS_FOLDER,
        PARTITION_DELETED_MARKER,
    },
    flush::manager::FlushManager,
    journal::{manager::JournalManager, Journal},
    partition::{name::is_valid_partition_name, PartitionHandleInner},
    version::Version,
    PartitionConfig, PartitionHandle,
};
use lsm_tree::{id::generate_segment_id, SequenceNumberCounter};
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};
use std_semaphore::Semaphore;

type Partitions = HashMap<Arc<str>, PartitionHandle>;

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
    // TODO: stop signal
}

impl Drop for KeyspaceInner {
    fn drop(&mut self) {
        log::trace!("Dropping Keyspace, trying to flush journal");

        if let Err(e) = self.journal.flush(true) {
            log::error!("Flush error on drop: {e:?}");
        }
    }
}

/// The keyspace houses multiple partitions (column families).
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
    /// the journal (and everything else) are (or should be) crash-safe.
    ///
    /// # Errors
    ///
    /// Returns error, if an IO error occured.
    pub fn persist(&self) -> crate::Result<()> {
        self.journal.flush(false)
    }

    /// Opens a keyspace in the given directory.
    ///
    /// # Errors
    ///
    /// Returns error, if an IO error occured.
    pub fn open(config: Config) -> crate::Result<Self> {
        log::debug!("Opening keyspace at {}", config.path.display());

        let keyspace = if config.path.join(FJALL_MARKER).try_exists()? {
            Self::recover(config)
        } else {
            Self::create_new(config)
        }?;

        keyspace.spawn_flush_worker();

        for _ in 0..4 {
            keyspace.spawn_compaction_worker();
        }

        if let Some(ms) = keyspace.config.fsync_ms {
            keyspace.spawn_fsync_thread(ms.into());
        }

        Ok(keyspace)
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
        config: PartitionConfig,
    ) -> crate::Result<PartitionHandle> {
        assert!(is_valid_partition_name(name));

        let partitions = self.partitions.write().expect("lock is poisoned");

        Ok(if let Some(partition) = partitions.get(name) {
            partition.clone()
        } else {
            log::debug!("Creating partition {name}");
            PartitionHandle::create_new(self.clone(), name.into(), config)?
        })
    }

    /// Returns `true` if the partition with the given name exists
    #[must_use]
    pub fn partition_exists(&self, name: &str) -> bool {
        self.partitions
            .read()
            .expect("lock is poisoned")
            .contains_key(name)
    }

    /// Recovers existing keyspace from directory
    #[allow(clippy::too_many_lines)]
    #[doc(hidden)]
    pub fn recover(config: Config) -> crate::Result<Self> {
        log::debug!("Recovering keyspace at {}", config.path.display());

        {
            let bytes = std::fs::read(config.path.join(FJALL_MARKER))?;

            if let Some(version) = Version::parse_file_header(&bytes) {
                if version != Version::V0 {
                    return Err(crate::Error::InvalidVersion(Some(version)));
                }
            } else {
                return Err(crate::Error::InvalidVersion(None));
            }
        }

        let journals_folder = config.path.join(JOURNALS_FOLDER);

        let active_journal = {
            let mut journal = None;

            for dirent in std::fs::read_dir(&journals_folder)? {
                let dirent = dirent?;

                if !dirent.path().join(FLUSH_MARKER).try_exists()? {
                    journal = Some(Journal::recover(dirent.path())?);
                }
            }

            journal
        };

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

        let partitions_folder = config.path.join(PARTITIONS_FOLDER);

        let mut journal_manager = JournalManager::new(journal.clone(), journal_path);

        journal_manager.disk_space_in_bytes =
            fs_extra::dir::get_size(&journals_folder).map_err(|e| {
                std::io::Error::new(std::io::ErrorKind::Other, format!("fs_extra error: {e:?}"))
            })?;

        let inner = KeyspaceInner {
            config,
            journal,
            partitions: Arc::new(RwLock::new(Partitions::with_capacity(10))),
            seqno: SequenceNumberCounter::default(),
            flush_manager: Arc::default(),
            journal_manager: Arc::new(RwLock::new(journal_manager)),
            flush_semaphore: Arc::new(Semaphore::new(0)),
            compaction_manager: CompactionManager::default(),
        };

        let keyspace = Self(Arc::new(inner));

        for dirent in std::fs::read_dir(partitions_folder)? {
            let dirent = dirent?;
            let partition_name = dirent.file_name();
            let partition_path = dirent.path();

            log::trace!("Recovering partition {:?}", partition_name);

            if partition_path.join(PARTITION_DELETED_MARKER).try_exists()? {
                log::debug!("Deleting deleted partition {:?}", partition_name);
                std::fs::remove_dir_all(partition_path)?;
                continue;
            }

            if !partition_path.join(".lsm").try_exists()? {
                log::debug!("Deleting uninitialized partition {:?}", partition_name);
                std::fs::remove_dir_all(partition_path)?;
                continue;
            }

            let partition_name = partition_name
                .to_str()
                .expect("should be valid partition name");

            let path = keyspace
                .config
                .path
                .join(PARTITIONS_FOLDER)
                .join(partition_name);

            let tree = lsm_tree::Config::new(path)
                .block_cache(keyspace.config.block_cache.clone())
                .open()?;

            let partition_inner = PartitionHandleInner {
                max_memtable_size: 8 * 1_024 * 1_024, // TODO:
                compaction_strategy: Arc::new(lsm_tree::compaction::SizeTiered), // TODO:
                keyspace: keyspace.clone(),
                name: partition_name.into(),
                tree,
            };
            let partition_inner = Arc::new(partition_inner);

            let partition = PartitionHandle(partition_inner);

            if let Some(recovered_memtable) = memtables.remove(partition_name) {
                log::trace!(
                    "Recovered previously active memtable for {:?}, with size: {} B",
                    partition_name,
                    recovered_memtable.size()
                );
                partition.tree.set_active_memtable(recovered_memtable);
            }

            let maybe_next_seqno = partition.tree.get_lsn().map(|x| x + 1).unwrap_or_default();
            keyspace
                .seqno
                .fetch_max(maybe_next_seqno, std::sync::atomic::Ordering::AcqRel);
            log::debug!("Keyspace seqno is now {}", keyspace.seqno.get());

            keyspace
                .partitions
                .write()
                .expect("lock is poisoned")
                .insert(partition_name.into(), partition);

            log::trace!("Recovered partition {:?}", partition_name);
        }

        let mut dirents =
            std::fs::read_dir(journals_folder)?.collect::<std::io::Result<Vec<_>>>()?;
        dirents.sort_by_key(std::fs::DirEntry::file_name);

        for dirent in dirents {
            let journal_path = dirent.path();

            if dirent.path().join(FLUSH_MARKER).try_exists()? {
                log::trace!("Requeueing sealed journal at {:?}", journal_path);

                let partitions_to_consider =
                    std::fs::read_to_string(journal_path.join(FLUSH_PARTITIONS_LIST))?;

                let partitions_to_consider = partitions_to_consider
                    .split('\n')
                    .filter(|x| !x.is_empty())
                    .map(|x| {
                        let mut splits = x.split(':');
                        let name = splits.next().expect("partition name should exist");
                        let lsn = splits.next().expect("lsn should exist");
                        let lsn = lsn
                            .parse::<lsm_tree::SeqNo>()
                            .expect("should be valid seqno");

                        (name, lsn)
                    })
                    .collect::<Vec<_>>();

                log::trace!(
                    "Journal contains data of {} partitions",
                    partitions_to_consider.len()
                );

                let mut partition_seqno_map = HashMap::default();
                let partitions_lock = keyspace.partitions.read().expect("lock is poisoned");

                for (partition_name, lsn) in partitions_to_consider {
                    let Some(partition) = partitions_lock.get(partition_name) else {
                        // Partition was probably deleted
                        log::trace!("Partition does not exist");
                        continue;
                    };

                    let Some(partition_lsn) = partition.tree.get_segment_lsn() else {
                        log::trace!("Partition has higher seqno, skipping");
                        continue;
                    };

                    if lsn > partition_lsn {
                        partition_seqno_map.insert(
                            partition_name.into(),
                            crate::journal::manager::PartitionSeqNo {
                                lsn: partition_lsn,
                                partition: partition.clone(),
                            },
                        );
                    }
                }

                let mut journal_manager_lock =
                    keyspace.journal_manager.write().expect("lock is poisoned");

                let mut flush_manager_lock =
                    keyspace.flush_manager.write().expect("lock is poisoned");

                let journal_size = fs_extra::dir::get_size(&journal_path).map_err(|e| {
                    std::io::Error::new(std::io::ErrorKind::Other, format!("{:?}", e.kind))
                })?;

                let partition_names_to_recover =
                    partition_seqno_map.keys().cloned().collect::<Vec<_>>();

                log::trace!("Recovering memtables for partitions: {partition_names_to_recover:#?}");

                let memtables =
                    Journal::recover_memtables(&journal_path, Some(&partition_names_to_recover))?;

                log::trace!("Recovered {} sealed memtables", memtables.len());

                journal_manager_lock.enqueue(crate::journal::manager::Item {
                    partition_seqnos: partition_seqno_map,
                    path: journal_path.clone(),
                    size_in_bytes: journal_size,
                });

                for (partition_name, sealed_memtable) in memtables {
                    let Some(partition) = partitions_lock.get(&partition_name) else {
                        // Should not happen
                        continue;
                    };

                    let memtable_id = generate_segment_id();
                    let sealed_memtable = Arc::new(sealed_memtable);

                    partition
                        .tree
                        .add_sealed_memtables(memtable_id.clone(), sealed_memtable.clone());

                    let maybe_next_seqno =
                        partition.tree.get_lsn().map(|x| x + 1).unwrap_or_default();
                    keyspace
                        .seqno
                        .fetch_max(maybe_next_seqno, std::sync::atomic::Ordering::AcqRel);
                    log::debug!("Keyspace seqno is now {}", keyspace.seqno.get());

                    flush_manager_lock.enqueue_task(
                        partition_name,
                        crate::flush::manager::Task {
                            id: memtable_id,
                            sealed_memtable,
                            partition: partition.clone(),
                        },
                    );
                }

                log::trace!("Requeued sealed journal at {:?}", journal_path);
            }
        }

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
            journal: journal.clone(),
            partitions: Arc::new(RwLock::new(Partitions::with_capacity(10))),
            seqno: SequenceNumberCounter::default(),
            flush_manager: Arc::default(),
            journal_manager: Arc::new(RwLock::new(JournalManager::new(
                journal,
                active_journal_path,
            ))),
            flush_semaphore: Arc::new(Semaphore::new(0)),
            compaction_manager: CompactionManager::default(),
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

    fn spawn_fsync_thread(&self, ms: usize) {
        let journal = self.journal.clone();

        std::thread::spawn(move || loop {
            log::trace!("fsync thread: sleeping {ms}ms");
            std::thread::sleep(std::time::Duration::from_millis(ms as u64));

            // TODO:
            /* if stop_signal.is_stopped() {
                log::debug!("fsync thread: exiting because tree is dropping");
                return;
            } */

            log::trace!("fsync thread: fsycing journal");
            if let Err(e) = journal.flush(false) {
                log::error!("Fsync failed: {e:?}");
            }
        });
    }

    fn spawn_compaction_worker(&self) {
        let keyspace = self.clone();
        std::thread::spawn(move || {
            crate::compaction::worker::run(&keyspace);
        });
    }

    fn spawn_flush_worker(&self) {
        let keyspace = self.clone();
        std::thread::spawn(move || {
            crate::flush::worker::run(&keyspace);
        });
    }
}
