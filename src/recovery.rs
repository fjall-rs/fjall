// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    file::{KEYSPACES_FOLDER, LSM_CURRENT_VERSION_MARKER},
    journal::{
        batch_reader::JournalBatchReader, manager::EvictionWatermark, reader::JournalReader,
    },
    keyspace::{
        apply_to_base_config, options::CreateOptions as KeyspaceCreateOptions, InternalKeyspaceId,
    },
    meta_keyspace::MetaKeyspace,
    Database, HashMap, Keyspace,
};
use lsm_tree::AbstractTree;
use std::{path::PathBuf, sync::Arc};

/// Recovers keyspaces
pub fn recover_keyspaces(db: &Database, meta_keyspace: &MetaKeyspace) -> crate::Result<()> {
    let keyspaces_folder = db.config.path.join(KEYSPACES_FOLDER);

    log::trace!("Recovering keyspaces in {}", keyspaces_folder.display());

    #[expect(clippy::expect_used)]
    let mut keyspaces_lock = db.keyspaces.write().expect("lock is poisoned");

    let mut highest_id = 1;

    for dirent in std::fs::read_dir(&keyspaces_folder)? {
        let dirent = dirent?;
        let keyspace_path = dirent.path();

        if dirent.file_type()?.is_file() {
            log::warn!(
                "Found stray file {} in keyspaces folder",
                keyspace_path.display(),
            );
            continue;
        }

        let keyspace_id = dirent
            .file_name()
            .to_str()
            .expect("should be valid keyspace name")
            .parse::<InternalKeyspaceId>()
            .expect("should be valid integer");

        // NOTE: Is meta keyspace
        if keyspace_id == 0 {
            continue;
        }

        highest_id = highest_id.max(keyspace_id);

        let Some(keyspace_name) = meta_keyspace.resolve_id(keyspace_id)? else {
            log::debug!("Deleting unreferenced keyspace id={keyspace_id}");
            std::fs::remove_dir_all(keyspace_path)?;
            continue;
        };

        log::trace!("Recovering keyspace {keyspace_id}");

        // NOTE: Check for marker, maybe the keyspace is not fully initialized
        if !keyspace_path
            .join(LSM_CURRENT_VERSION_MARKER)
            .try_exists()?
        {
            log::debug!("Deleting uninitialized keyspace {keyspace_name:?}");
            std::fs::remove_dir_all(keyspace_path)?;
            continue;
        }

        let path = keyspaces_folder.join(keyspace_id.to_string());

        let recovered_config = KeyspaceCreateOptions::from_kvs(keyspace_id, &db.meta_keyspace)?;

        let base_config = lsm_tree::Config::new(
            path,
            db.supervisor.seqno.clone(),
            db.supervisor.snapshot_tracker.get_ref(),
        )
        .use_descriptor_table(db.config.descriptor_table.clone())
        .use_cache(db.config.cache.clone());

        let base_config = apply_to_base_config(base_config, &recovered_config);

        let tree = base_config.open()?;

        let keyspace = Keyspace::from_database(
            keyspace_id,
            db,
            tree,
            keyspace_name.clone(),
            recovered_config,
        );

        // Add keyspace to dictionary
        keyspaces_lock.insert(keyspace_name.clone(), keyspace.clone());

        log::trace!("Recovered keyspace {keyspace_name:?}");
    }

    db.keyspace_id_counter.set(highest_id + 1);

    Ok(())
}

pub fn recover_sealed_memtables(
    db: &Database,
    sealed_journal_paths: &[PathBuf],
) -> crate::Result<()> {
    #[expect(clippy::expect_used)]
    let mut journal_manager_lock = db
        .supervisor
        .journal_manager
        .write()
        .expect("lock is poisoned");

    #[expect(clippy::expect_used)]
    let keyspaces_lock = db.keyspaces.read().expect("lock is poisoned");

    for journal_path in sealed_journal_paths {
        log::debug!("Recovering sealed journal: {}", journal_path.display());

        let journal_size = journal_path.metadata()?.len();

        log::debug!("Reading sealed journal at {}", journal_path.display());

        let raw_reader = JournalReader::new(journal_path)?;
        let reader = JournalBatchReader::new(raw_reader);

        let mut watermarks: HashMap<InternalKeyspaceId, EvictionWatermark> = HashMap::default();

        for batch in reader {
            let batch = batch?;

            for item in batch.items {
                let Some(keyspace_name) = db.meta_keyspace.resolve_id(item.keyspace_id)? else {
                    continue;
                };

                let Some(handle) = keyspaces_lock.get(&keyspace_name) else {
                    continue;
                };

                let tree = &handle.tree;

                watermarks
                    .entry(item.keyspace_id)
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
                    lsm_tree::ValueType::Indirection => {
                        unreachable!()
                    }
                }
            }
        }

        log::debug!("Sealing recovered memtables");
        let mut recovered_count = 0;

        for wm in watermarks.values() {
            let tree = &wm.keyspace.tree;

            let keyspace_lsn = tree.get_highest_persisted_seqno();

            // IMPORTANT: Only apply sealed memtables to keyspaces
            // that have a lower seqno to avoid double flushing
            let should_skip_sealed_memtable =
                keyspace_lsn.is_some_and(|keyspace_lsn| keyspace_lsn >= wm.lsn);

            if should_skip_sealed_memtable {
                log::trace!(
                    "Keyspace {:?} has higher seqno ({keyspace_lsn:?}), skipping",
                    wm.keyspace.name,
                );

                tree.clear_active_memtable();
            } else if let Some(sealed_memtable) = tree.rotate_memtable() {
                log::trace!("Sealed active memtable of keyspace {:?}", wm.keyspace.name);

                assert_eq!(
                    Some(wm.lsn),
                    sealed_memtable.get_highest_seqno(),
                    "memtable lsn does not match what was recovered - this is a bug",
                );

                // Maybe the memtable has a higher seqno, so try to set to maximum
                let maybe_next_seqno = tree.get_highest_seqno().map(|x| x + 1).unwrap_or_default();
                db.supervisor.seqno.fetch_max(maybe_next_seqno);

                log::debug!("Database seqno is now {}", db.supervisor.seqno.get());

                // IMPORTANT: Add sealed memtable size to current write buffer size
                db.supervisor
                    .write_buffer_size
                    .allocate(sealed_memtable.size());

                // TODO: unit test write buffer size after recovery

                // IMPORTANT: Add sealed memtable to flush manager, so it can be flushed
                db.supervisor
                    .flush_manager
                    .enqueue(Arc::new(crate::flush::Task {
                        keyspace: wm.keyspace.clone(),
                    }));

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
