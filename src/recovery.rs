// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    batch::KeyspaceKey,
    file::{KEYSPACES_FOLDER, KEYSPACE_CONFIG_FILE, KEYSPACE_DELETED_MARKER, LSM_MANIFEST_FILE},
    journal::{
        batch_reader::JournalBatchReader, manager::EvictionWatermark, reader::JournalReader,
    },
    keyspace::options::CreateOptions as KeyspaceCreateOptions,
    Database, HashMap, Keyspace,
};
use lsm_tree::{AbstractTree, AnyTree};
use std::{fs::File, path::PathBuf};

/// Recovers keyspaces
pub fn recover_keyspaces(db: &Database) -> crate::Result<()> {
    use lsm_tree::coding::Decode;

    let keyspaces_folder = db.config.path.join(KEYSPACES_FOLDER);

    log::trace!("Recovering keyspaces in {}", keyspaces_folder.display());

    #[allow(clippy::significant_drop_tightening)]
    let mut keyspaces_lock = db.keyspaces.write().expect("lock is poisoned");

    for dirent in std::fs::read_dir(&keyspaces_folder)? {
        let dirent = dirent?;
        let keyspace_name = dirent.file_name();
        let keyspace_path = dirent.path();

        if dirent.file_type()?.is_file() {
            log::warn!(
                "Found stray file {} in keyspaces folder",
                keyspace_path.display(),
            );
            continue;
        }

        log::trace!("Recovering keyspace {}", keyspace_name.display());

        // NOTE: Check deletion marker
        if keyspace_path.join(KEYSPACE_DELETED_MARKER).try_exists()? {
            log::debug!("Deleting deleted keyspace {}", keyspace_name.display());

            // IMPORTANT: First, delete the manifest,
            // once that is deleted, the keyspace is treated as uninitialized
            // even if the .deleted marker is removed
            //
            // This is important, because if somehow `remove_dir_all` ends up
            // deleting the `.deleted` marker first, we would end up resurrecting
            // the keyspace
            let manifest_file = keyspace_path.join(LSM_MANIFEST_FILE);
            if manifest_file.try_exists()? {
                std::fs::remove_file(manifest_file)?;
            }

            std::fs::remove_dir_all(keyspace_path)?;
            continue;
        }

        // NOTE: Check for marker, maybe the keyspace is not fully initialized
        if !keyspace_path.join(LSM_MANIFEST_FILE).try_exists()? {
            log::debug!(
                "Deleting uninitialized keyspace {}",
                keyspace_name.display(),
            );
            std::fs::remove_dir_all(keyspace_path)?;
            continue;
        }

        let keyspace_name: KeyspaceKey = keyspace_name
            .to_str()
            .expect("should be valid keyspace name")
            .into();

        let path = keyspaces_folder.join(&*keyspace_name);

        let mut config_file = File::open(keyspace_path.join(KEYSPACE_CONFIG_FILE))?;
        let recovered_config = KeyspaceCreateOptions::decode_from(&mut config_file)?;

        let mut base_config = lsm_tree::Config::new(path)
            .descriptor_table(db.config.descriptor_table.clone())
            .use_cache(db.config.cache.clone());

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

        let is_blob_tree = keyspace_path
            .join(lsm_tree::file::BLOBS_FOLDER)
            .try_exists()?;

        let tree = if is_blob_tree {
            AnyTree::Blob(base_config.open_as_blob_tree()?)
        } else {
            AnyTree::Standard(base_config.open()?)
        };

        let keyspace = Keyspace::from_database(db, tree, keyspace_name.clone(), recovered_config);

        // Add keyspace to dictionary
        keyspaces_lock.insert(keyspace_name.clone(), keyspace.clone());

        log::trace!("Recovered keyspace {keyspace_name:?}");
    }

    Ok(())
}

#[allow(clippy::too_many_lines)]
pub fn recover_sealed_memtables(
    db: &Database,
    sealed_journal_paths: &[PathBuf],
) -> crate::Result<()> {
    #[allow(clippy::significant_drop_tightening)]
    let mut flush_manager_lock = db.flush_manager.write().expect("lock is poisoned");

    #[allow(clippy::significant_drop_tightening)]
    let mut journal_manager_lock = db.journal_manager.write().expect("lock is poisoned");

    #[allow(clippy::significant_drop_tightening)]
    let keyspaces_lock = db.keyspaces.read().expect("lock is poisoned");

    for journal_path in sealed_journal_paths {
        log::debug!("Recovering sealed journal: {}", journal_path.display());

        let journal_size = journal_path.metadata()?.len();

        log::debug!("Reading sealed journal at {}", journal_path.display());

        let raw_reader = JournalReader::new(journal_path)?;
        let reader = JournalBatchReader::new(raw_reader);

        let mut watermarks: HashMap<KeyspaceKey, EvictionWatermark> = HashMap::default();

        for batch in reader {
            let batch = batch?;

            for item in batch.items {
                if let Some(handle) = keyspaces_lock.get(&item.keyspace) {
                    let tree = &handle.tree;

                    watermarks
                        .entry(item.keyspace)
                        .and_modify(|prev| {
                            prev.lsn = prev.lsn.max(batch.seqno);
                        })
                        .or_insert_with(|| EvictionWatermark {
                            keyspace: handle.clone(),
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
            let tree = &handle.keyspace.tree;

            let keyspace_lsn = tree.get_highest_persisted_seqno();

            // IMPORTANT: Only apply sealed memtables to keyspaces
            // that have a lower seqno to avoid double flushing
            let should_skip_sealed_memtable =
                keyspace_lsn.is_some_and(|keyspace_lsn| keyspace_lsn >= handle.lsn);

            if should_skip_sealed_memtable {
                handle.keyspace.tree.clear_active_memtable();

                log::trace!(
                    "Keyspace {} has higher seqno ({keyspace_lsn:?}), skipping",
                    handle.keyspace.name
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
                    handle.keyspace.name,
                    sealed_memtable.len(),
                );

                // Maybe the memtable has a higher seqno, so try to set to maximum
                let maybe_next_seqno = tree.get_highest_seqno().map(|x| x + 1).unwrap_or_default();
                db.seqno.fetch_max(maybe_next_seqno);
                log::debug!("Database seqno is now {}", db.seqno.get());

                // IMPORTANT: Add sealed memtable size to current write buffer size
                db.write_buffer_manager.allocate(sealed_memtable.size());

                // TODO: unit test write buffer size after recovery

                // IMPORTANT: Add sealed memtable to flush manager, so it can be flushed
                flush_manager_lock.enqueue_task(
                    handle.keyspace.name.clone(),
                    crate::flush::manager::Task {
                        id: memtable_id,
                        sealed_memtable,
                        keyspace: handle.keyspace.clone(),
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

        log::debug!("Requeued sealed journal at {}", journal_path.display());
    }

    Ok(())
}
