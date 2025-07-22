// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    batch::PartitionKey,
    file::{LSM_MANIFEST_FILE, PARTITIONS_FOLDER, PARTITION_CONFIG_FILE, PARTITION_DELETED_MARKER},
    journal::{
        batch_reader::JournalBatchReader, manager::EvictionWatermark, reader::JournalReader,
    },
    partition::options::CreateOptions as PartitionCreateOptions,
    HashMap, Keyspace, PartitionHandle,
};
use lsm_tree::{AbstractTree, AnyTree};
use std::{fs::File, path::PathBuf};

/// Recovers partitions
pub fn recover_partitions(keyspace: &Keyspace) -> crate::Result<()> {
    use lsm_tree::coding::Decode;

    let partitions_folder = keyspace.config.path.join(PARTITIONS_FOLDER);

    log::trace!("Recovering partitions in {partitions_folder:?}");

    #[allow(clippy::significant_drop_tightening)]
    let mut partitions_lock = keyspace.partitions.write().expect("lock is poisoned");

    for dirent in std::fs::read_dir(&partitions_folder)? {
        let dirent = dirent?;
        let partition_name = dirent.file_name();
        let partition_path = dirent.path();

        if dirent.file_type()?.is_file() {
            log::warn!("Found stray file {partition_path:?} in partitions folder");
            continue;
        }

        log::trace!("Recovering partition {partition_name:?}");

        // NOTE: Check deletion marker
        if partition_path.join(PARTITION_DELETED_MARKER).try_exists()? {
            log::debug!("Deleting deleted partition {partition_name:?}");

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
            log::debug!("Deleting uninitialized partition {partition_name:?}");
            std::fs::remove_dir_all(partition_path)?;
            continue;
        }

        let partition_name: PartitionKey = partition_name
            .to_str()
            .expect("should be valid partition name")
            .into();

        let path = partitions_folder.join(&*partition_name);

        let mut config_file = File::open(partition_path.join(PARTITION_CONFIG_FILE))?;
        let recovered_config = PartitionCreateOptions::decode_from(&mut config_file)?;

        let mut base_config = lsm_tree::Config::new(path)
            .descriptor_table(keyspace.config.descriptor_table.clone())
            .use_cache(keyspace.config.cache.clone());

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

        let partition = PartitionHandle::from_keyspace(
            keyspace,
            tree,
            partition_name.clone(),
            recovered_config,
        );

        // Add partition to dictionary
        partitions_lock.insert(partition_name.clone(), partition.clone());

        log::trace!("Recovered partition {partition_name:?}");
    }

    Ok(())
}

#[allow(clippy::too_many_lines)]
pub fn recover_sealed_memtables(
    keyspace: &Keyspace,
    sealed_journal_paths: &[PathBuf],
) -> crate::Result<()> {
    #[allow(clippy::significant_drop_tightening)]
    let mut flush_manager_lock = keyspace.flush_manager.write().expect("lock is poisoned");

    #[allow(clippy::significant_drop_tightening)]
    let mut journal_manager_lock = keyspace.journal_manager.write().expect("lock is poisoned");

    #[allow(clippy::significant_drop_tightening)]
    let partitions_lock = keyspace.partitions.read().expect("lock is poisoned");

    for journal_path in sealed_journal_paths {
        log::debug!("Recovering sealed journal: {journal_path:?}");

        let journal_size = journal_path.metadata()?.len();

        log::debug!("Reading sealed journal at {journal_path:?}");

        let raw_reader = JournalReader::new(journal_path)?;
        let reader = JournalBatchReader::new(raw_reader);

        let mut watermarks: HashMap<PartitionKey, EvictionWatermark> = HashMap::default();

        for batch in reader {
            let batch = batch?;

            for item in batch.items {
                if let Some(handle) = partitions_lock.get(&item.partition) {
                    let tree = &handle.tree;

                    watermarks
                        .entry(item.partition)
                        .and_modify(|prev| {
                            prev.lsn = prev.lsn.max(batch.seqno);
                        })
                        .or_insert_with(|| EvictionWatermark {
                            partition: handle.clone(),
                            lsn: batch.seqno,
                        });

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

        for handle in watermarks.values() {
            let tree = &handle.partition.tree;

            let partition_lsn = tree.get_highest_persisted_seqno();

            // IMPORTANT: Only apply sealed memtables to partitions
            // that have a lower seqno to avoid double flushing
            let should_skip_sealed_memtable =
                partition_lsn.is_some_and(|partition_lsn| partition_lsn >= handle.lsn);

            if should_skip_sealed_memtable {
                handle.partition.tree.clear_active_memtable();

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
                let maybe_next_seqno = tree.get_highest_seqno().map(|x| x + 1).unwrap_or_default();

                keyspace
                    .seqno
                    .fetch_max(maybe_next_seqno, std::sync::atomic::Ordering::AcqRel);

                log::debug!("Keyspace seqno is now {}", keyspace.seqno.get());

                // IMPORTANT: Add sealed memtable size to current write buffer size
                keyspace
                    .write_buffer_manager
                    .allocate(sealed_memtable.size().into());

                // TODO: unit test write buffer size after recovery

                // IMPORTANT: Add sealed memtable to flush manager, so it can be flushed
                flush_manager_lock.enqueue_task(
                    handle.partition.name.clone(),
                    crate::flush::manager::Task {
                        id: memtable_id,
                        sealed_memtable,
                        partition: handle.partition.clone(),
                    },
                );

                recovered_count += 1;
            }
        }

        log::debug!("Recovered {recovered_count} sealed memtables");

        // IMPORTANT: Add sealed journal to journal manager
        journal_manager_lock.enqueue(crate::journal::manager::Item {
            watermarks: watermarks.into_values().collect(),
            path: journal_path.clone(),
            size_in_bytes: journal_size,
        });

        log::debug!("Requeued sealed journal at {journal_path:?}");
    }

    Ok(())
}
