use crate::{
    legacy::v3::journal::{reader::JournalReader, JournalBatchReader},
    Database, HashMap,
};
use lsm_tree::AbstractTree;
use std::path::{Path, PathBuf};

pub fn recover_sealed_memtables(db: &Database, journal_paths: &[PathBuf]) -> crate::Result<()> {
    log::debug!("Replaying v3 journals into memtables");

    let mut entry_count = 0;
    let mut highest_encountered_keyspace_id = 0;

    #[expect(clippy::expect_used)]
    let keyspace_registy = db.supervisor.keyspaces.read().expect("lock is poisoned");

    let mut persisted_seqnos: HashMap<_, _> = HashMap::default();

    for journal_path in journal_paths {
        log::debug!("Recovering sealed journal: {}", journal_path.display());

        log::debug!("Replaying sealed journal at {}", journal_path.display());

        let raw_reader = JournalReader::new(journal_path)?;
        let reader = JournalBatchReader::new(raw_reader);

        for batch in reader {
            let batch = batch?;

            for item in batch.items {
                entry_count += 1;

                highest_encountered_keyspace_id =
                    highest_encountered_keyspace_id.max(item.keyspace_id);

                let Some(handle) = keyspace_registy
                    .values()
                    .find(|k| k.id() == item.keyspace_id)
                else {
                    continue;
                };

                let tree = &handle.tree;

                // NOTE: Skip items that were already flushed, otherwise the next flush
                // writes them again into a table that overlaps the existing one
                let persisted_seqno = *persisted_seqnos
                    .entry(item.keyspace_id)
                    .or_insert_with(|| tree.get_highest_persisted_seqno());

                if persisted_seqno.is_some_and(|seqno| batch.seqno <= seqno) {
                    continue;
                }

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

            for &keyspace_id in &batch.cleared_keyspaces {
                let Some(handle) = keyspace_registy.values().find(|k| k.id() == keyspace_id) else {
                    continue;
                };

                log::debug!("Clearing keyspace {}", handle.name());

                if let Some(super_version) = handle.tree.clear_active_memtable() {
                    handle.version_set_provider.overwrite(super_version);
                }

                persisted_seqnos.remove(&keyspace_id);
            }
        }
    }

    log::debug!("Replayed {entry_count} entries from v3 journals");

    db.keyspace_id_counter
        .fetch_max(highest_encountered_keyspace_id + 1);

    log::trace!(
        "Set keyspace id counter to {}",
        db.keyspace_id_counter.get(),
    );

    Ok(())
}

pub type JournalId = u64;

pub fn nuke_journal(path: &Path) -> crate::Result<()> {
    let journal_files = find_journal_files(path)?;

    for (_, p) in journal_files {
        log::debug!("Removing old v3 journal file: {}", p.display());
        std::fs::remove_file(p)?;
    }

    Ok(())
}

pub fn find_journal_files(path: &Path) -> crate::Result<Vec<(JournalId, PathBuf)>> {
    let mut max_journal_id: JournalId = 0;
    let mut journal_fragments = Vec::<(JournalId, PathBuf)>::new();

    for dirent in std::fs::read_dir(path)? {
        let dirent = dirent?;
        let path = dirent.path();
        let filename = dirent.file_name();

        let Some(filename) = filename.to_str() else {
            log::error!("Invalid journal file name: {}", filename.display());
            return Err(crate::Error::JournalRecovery(
                crate::JournalRecoveryError::InvalidFileName,
            ));
        };

        if !std::path::Path::new(filename)
            .extension()
            .is_some_and(|ext| ext.eq_ignore_ascii_case("jnl"))
        {
            continue;
        }

        assert!(dirent.file_type()?.is_file());

        let Some(basename) = filename.strip_suffix(".jnl") else {
            log::error!("Invalid journal file name: {filename}");
            return Err(crate::Error::JournalRecovery(
                crate::JournalRecoveryError::InvalidFileName,
            ));
        };

        let journal_id = basename.parse::<JournalId>().map_err(|_| {
            log::error!("Invalid journal file name: {filename}");
            crate::Error::JournalRecovery(crate::JournalRecoveryError::InvalidFileName)
        })?;

        max_journal_id = max_journal_id.max(journal_id);

        journal_fragments.push((journal_id, path));
    }

    // NOTE: Sort ascending, so the last item is the active journal
    journal_fragments.sort_by_key(|(a, _)| *a);

    Ok(journal_fragments)
}

pub fn do_full_v3_journal_recovery(db: &Database) -> crate::Result<()> {
    let journal_recovery = find_journal_files(db.path())?;
    log::debug!("Found v3 journals: {journal_recovery:#?}");

    // Recover sealed memtables by walking through old v3 journals
    recover_sealed_memtables(
        db,
        &journal_recovery
            .into_iter()
            .map(|(_, x)| x)
            .collect::<Vec<_>>(),
    )?;

    Ok(())
}
