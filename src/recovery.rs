use crate::{
    file::{LSM_MANIFEST_FILE, PARTITIONS_FOLDER, PARTITION_CONFIG_FILE, PARTITION_DELETED_MARKER},
    flush::manager::Task,
    flush_tracker::FlushTracker,
    journal::{
        batch_reader::JournalBatchReader,
        manager::{EvictionWatermark, Item},
        reader::JournalReader,
    },
    keyspace::Partitions,
    partition::options::CreateOptions as PartitionCreateOptions,
    Keyspace, PartitionHandle,
};
use lsm_tree::{AbstractTree, AnyTree, SequenceNumberCounter};
use std::{fs::File, path::PathBuf, sync::RwLock};

impl Keyspace {
    /// Recovers partitions
    pub(crate) fn recover_partitions(&self) -> crate::Result<()> {
        use lsm_tree::coding::Decode;

        let partitions_folder = self.config.path.join(PARTITIONS_FOLDER);

        #[allow(clippy::significant_drop_tightening)]
        let mut partitions_lock = self.partitions.write().expect("lock is poisoned");

        for dirent in std::fs::read_dir(&partitions_folder)? {
            let dirent = dirent?;
            let partition_name = dirent.file_name();
            let partition_path = dirent.path();

            assert!(
                dirent.file_type()?.is_dir(),
                "Found stray file in partitions folder",
            );

            log::trace!("Recovering partition {:?}", partition_name);

            // NOTE: Check deletion marker
            if partition_path.join(PARTITION_DELETED_MARKER).try_exists()? {
                log::debug!("Deleting deleted partition {:?}", partition_name);

                // IMPORTANT: First, delete the manifest,
                // once that is deleted, the partition is treated as uninitialized
                // even if the .deleted marker is removed
                //
                // This is important, because if somehow `remove_dir_all` ends up
                // deleting the `.deleted` marker first, we would end up resurrecting
                // the partition
                let manifest_file = partition_path.join(LSM_MANIFEST_FILE);
                if manifest_file.try_exists()? {
                    std::fs::remove_file(manifest_file)?;
                }

                std::fs::remove_dir_all(partition_path)?;
                continue;
            }

            // NOTE: Check for marker, maybe the partition is not fully initialized
            if !partition_path.join(LSM_MANIFEST_FILE).try_exists()? {
                log::debug!("Deleting uninitialized partition {:?}", partition_name);
                std::fs::remove_dir_all(partition_path)?;
                continue;
            }

            let partition_name = partition_name
                .to_str()
                .expect("should be valid partition name");

            let path = partitions_folder.join(partition_name);

            let mut config_file = File::open(partition_path.join(PARTITION_CONFIG_FILE))?;
            let recovered_config = PartitionCreateOptions::decode_from(&mut config_file)?;

            let mut base_config = lsm_tree::Config::new(path)
                .descriptor_table(self.config.descriptor_table.clone())
                .block_cache(self.config.block_cache.clone())
                .blob_cache(self.config.blob_cache.clone());

            base_config.bloom_bits_per_key = recovered_config.bloom_bits_per_key;
            base_config.data_block_size = recovered_config.data_block_size;
            base_config.index_block_size = recovered_config.index_block_size;
            base_config.bloom_bits_per_key = recovered_config.bloom_bits_per_key;
            base_config.compression = recovered_config.compression;

            if let Some(kv_opts) = &recovered_config.kv_separation {
                base_config = base_config
                    .blob_compression(kv_opts.compression)
                    .blob_file_separation_threshold(kv_opts.separation_threshold)
                    .blob_file_target_size(kv_opts.file_target_size);
            }

            let is_blob_tree = partition_path
                .join(lsm_tree::file::BLOBS_FOLDER)
                .try_exists()?;

            let tree = if is_blob_tree {
                AnyTree::Blob(base_config.open_as_blob_tree()?)
            } else {
                AnyTree::Standard(base_config.open()?)
            };

            let partition =
                PartitionHandle::from_keyspace(self, tree, partition_name.into(), recovered_config);

            // Add partition to dictionary
            partitions_lock.insert(partition_name.into(), partition.clone());

            log::trace!("Recovered partition {:?}", partition_name);
        }

        Ok(())
    }
}

impl FlushTracker {
    #[allow(clippy::too_many_lines)]
    pub fn recover_sealed_memtables(
        &self,
        partitions: &RwLock<Partitions>,
        seqno: &SequenceNumberCounter,
        sealed_journal_paths: impl Iterator<Item = PathBuf>,
    ) -> crate::Result<()> {
        #[allow(clippy::significant_drop_tightening)]
        let partitions_lock = partitions.read().expect("lock is poisoned");

        for journal_path in sealed_journal_paths {
            log::debug!("Recovering sealed journal: {journal_path:?}");

            let journal_size = journal_path.metadata()?.len();

            log::debug!("Reading sealed journal at {journal_path:?}");

            let raw_reader = JournalReader::new(&journal_path)?;
            let reader = JournalBatchReader::new(raw_reader);

            let mut watermarks: Vec<EvictionWatermark> = Vec::new();

            for batch in reader {
                let batch = batch?;

                for item in batch.items {
                    if let Some(handle) = partitions_lock.get(&item.partition) {
                        let tree = &handle.tree;

                        match watermarks.binary_search_by(|watermark| {
                            watermark.partition.name.cmp(&item.partition)
                        }) {
                            Ok(index) => {
                                if let Some(prev) = watermarks.get_mut(index) {
                                    prev.lsn = prev.lsn.max(batch.seqno);
                                }
                            }
                            Err(index) => {
                                watermarks.insert(
                                    index,
                                    EvictionWatermark {
                                        partition: handle.clone(),
                                        lsn: batch.seqno,
                                    },
                                );
                            }
                        };

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

            log::debug!("Sealing recovered memtables");
            let mut recovered_count = 0;

            for handle in &watermarks {
                let tree = &handle.partition.tree;

                let partition_lsn = tree.get_highest_persisted_seqno();

                // IMPORTANT: Only apply sealed memtables to partitions
                // that have a lower seqno to avoid double flushing
                let should_skip_sealed_memtable =
                    partition_lsn.map_or(false, |partition_lsn| partition_lsn >= handle.lsn);

                if should_skip_sealed_memtable {
                    handle.partition.tree.lock_active_memtable().clear();

                    log::trace!(
                        "Partition {} has higher seqno ({partition_lsn:?}), skipping",
                        handle.partition.name
                    );
                    continue;
                }

                if let Some((memtable_id, sealed_memtable)) = tree.rotate_memtable() {
                    assert_eq!(
                        Some(handle.lsn),
                        sealed_memtable.get_highest_seqno(),
                        "memtable lsn does not match what was recovered - this is a bug"
                    );

                    log::trace!(
                        "sealed memtable of {} has {} items",
                        handle.partition.name,
                        sealed_memtable.len(),
                    );

                    // Maybe the memtable has a higher seqno, so try to set to maximum
                    let maybe_next_seqno =
                        tree.get_highest_seqno().map(|x| x + 1).unwrap_or_default();

                    seqno.fetch_max(maybe_next_seqno, std::sync::atomic::Ordering::AcqRel);

                    log::debug!("Keyspace seqno is now {}", seqno.get());

                    // IMPORTANT: Add sealed memtable size to current write buffer size
                    self.increment_buffer_size(sealed_memtable.size().into());

                    // TODO: unit test write buffer size after recovery

                    // IMPORTANT: Add sealed memtable to flush manager, so it can be flushed
                    self.enqueue_task(Task {
                        id: memtable_id,
                        sealed_memtable,
                        partition: handle.partition.clone(),
                    });

                    recovered_count += 1;
                };
            }

            log::debug!("Recovered {recovered_count} sealed memtables");

            // IMPORTANT: Add sealed journal to journal manager
            self.enqueue_item(Item {
                watermarks,
                path: journal_path.clone(),
                size_in_bytes: journal_size,
            });

            log::debug!("Requeued sealed journal at {:?}", journal_path);
        }

        Ok(())
    }
}
