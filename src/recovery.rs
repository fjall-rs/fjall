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
use std::path::PathBuf;

// Recovery must not replay an old oversized journal into one giant memtable.
// The per-keyspace setting may be much larger than a small instance can hold,
// so cap each synchronous recovery flush independently of normal write tuning.
const MAX_RECOVERY_MEMTABLE_BYTES: u64 = 8 * 1024 * 1024;
const MAX_RECOVERY_ACTIVE_BYTES: u64 = 16 * 1024 * 1024;

/// Tracks active replay buffers without scanning every keyspace after every
/// journal batch. Small batches are applied in full before flushing. Large
/// verified batches may flush while their entries are replayed; the source
/// journal remains available for a complete replay after interruption.
#[derive(Default)]
pub struct RecoveryBudget {
    active_sizes: HashMap<InternalKeyspaceId, u64>,
    total_active_bytes: u64,
}

impl RecoveryBudget {
    fn account(&mut self, keyspace: &Keyspace) {
        let size = keyspace.tree.active_memtable().size();
        let previous = self.active_sizes.insert(keyspace.id(), size).unwrap_or(0);
        self.total_active_bytes = self
            .total_active_bytes
            .saturating_sub(previous)
            .saturating_add(size);
    }

    pub fn after_batch<'a>(
        &mut self,
        db: &Database,
        touched: impl IntoIterator<Item = &'a Keyspace>,
        keyspaces: impl IntoIterator<Item = &'a Keyspace>,
    ) -> crate::Result<()> {
        for keyspace in touched {
            flush_recovery_chunk(db, keyspace, false)?;
            self.account(keyspace);
        }

        if self.total_active_bytes > MAX_RECOVERY_ACTIVE_BYTES {
            for keyspace in keyspaces {
                if keyspace.tree.active_memtable().size() > 0 {
                    flush_recovery_chunk(db, keyspace, true)?;
                    self.account(keyspace);
                }
                if self.total_active_bytes <= MAX_RECOVERY_ACTIVE_BYTES {
                    break;
                }
            }
        }
        Ok(())
    }
}

/// Flush a replayed chunk before its journal can be considered for eviction.
/// The source journal stays on disk throughout recovery, so an interrupted
/// replay can safely start again from the beginning on the next open.
pub fn flush_recovery_chunk(db: &Database, keyspace: &Keyspace, force: bool) -> crate::Result<()> {
    let budget = keyspace
        .config
        .max_memtable_size
        .min(MAX_RECOVERY_MEMTABLE_BYTES);
    if keyspace.tree.active_memtable().size() <= budget && !force {
        return Ok(());
    }

    if let Some(sealed) = keyspace.tree.rotate_memtable() {
        db.supervisor.write_buffer_size.allocate(sealed.size());
    }

    let flush_lock = keyspace.tree.get_flush_lock();
    if let Some(flushed_bytes) = keyspace.tree.flush(&flush_lock, 0)? {
        db.supervisor.write_buffer_size.free(flushed_bytes);
    }
    drop(flush_lock);

    // Each flush adds a manifest version. During recovery there are no user
    // snapshots, but the snapshot tracker otherwise stays at watermark zero
    // until open returns. Zero disables version GC and retains every flushed
    // memtable, defeating the memory bound.
    db.supervisor
        .snapshot_tracker
        .set(db.supervisor.seqno.get());
    db.supervisor.snapshot_tracker.pullup();
    keyspace.tree.get_version_history_lock().maintenance(
        keyspace.path(),
        db.supervisor.snapshot_tracker.get_seqno_safe_to_gc(),
    )?;
    Ok(())
}

/// Recovers keyspaces
pub fn recover_keyspaces(db: &Database, meta_keyspace: &MetaKeyspace) -> crate::Result<()> {
    let keyspaces_folder = db.config.path.join(KEYSPACES_FOLDER);

    log::trace!("Recovering keyspaces in {}", keyspaces_folder.display());

    #[expect(clippy::expect_used)]
    let mut keyspaces_lock = db.supervisor.keyspaces.write().expect("lock is poisoned");

    let mut highest_keyspace_id = 1;

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

        highest_keyspace_id = highest_keyspace_id.max(keyspace_id);

        let Some(keyspace_name) = meta_keyspace.resolve_id(keyspace_id)? else {
            log::debug!("Deleting unreferenced keyspace id={keyspace_id}");
            std::fs::remove_dir_all(keyspace_path)?;
            continue;
        };

        log::trace!("Recovering keyspace with id={keyspace_id}");

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

        let mut recovered_config = KeyspaceCreateOptions::from_kvs(keyspace_id, &db.meta_keyspace)?;

        // Install compaction filter factory if needed
        if let Some(f) = db
            .config
            .compaction_filter_factory_assigner
            .as_ref()
            .and_then(|f| f(&keyspace_name))
        {
            recovered_config = recovered_config.with_compaction_filter_factory(f);
        }

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

    db.keyspace_id_counter.set(highest_keyspace_id + 1);

    log::trace!(
        "Set keyspace id counter to {}",
        db.keyspace_id_counter.get(),
    );

    Ok(())
}

#[expect(clippy::too_many_lines)]
pub fn recover_sealed_memtables(
    db: &Database,
    sealed_journal_paths: &[PathBuf],
) -> crate::Result<()> {
    let mut highest_encountered_keyspace_id = 0;

    #[expect(clippy::expect_used)]
    let mut journal_manager_lock = db
        .supervisor
        .journal_manager
        .write()
        .expect("lock is poisoned");

    #[expect(clippy::expect_used)]
    let keyspaces_lock = db.supervisor.keyspaces.read().expect("lock is poisoned");

    for journal_path in sealed_journal_paths {
        log::debug!("Recovering sealed journal: {}", journal_path.display());

        let journal_size = journal_path.metadata()?.len();

        log::debug!("Replaying sealed journal at {}", journal_path.display());

        let raw_reader =
            JournalReader::new_with_value_limit(journal_path, db.config.max_journal_value_bytes)?;
        let reader = JournalBatchReader::new(raw_reader);

        let mut watermarks: HashMap<_, EvictionWatermark> = HashMap::default();
        let mut touched_keyspaces = Vec::new();
        let mut recovery_budget = RecoveryBudget::default();

        for batch in reader {
            let batch = batch?;
            db.supervisor.seqno.fetch_max(batch.seqno + 1);
            touched_keyspaces.clear();
            let streaming = batch.items.is_streaming();

            for item in batch.items {
                let item = item?;
                highest_encountered_keyspace_id =
                    highest_encountered_keyspace_id.max(item.keyspace_id);

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

                if !touched_keyspaces
                    .iter()
                    .any(|touched: &&Keyspace| touched.id() == handle.id())
                {
                    touched_keyspaces.push(handle);
                }
                if streaming {
                    recovery_budget.after_batch(
                        db,
                        std::iter::once(handle),
                        keyspaces_lock.values(),
                    )?;
                }
            }

            for keyspace_id in &batch.cleared_keyspaces {
                let Some(keyspace_name) = db.meta_keyspace.resolve_id(*keyspace_id)? else {
                    continue;
                };

                let Some(handle) = keyspaces_lock.get(&keyspace_name) else {
                    continue;
                };

                watermarks
                    .entry(*keyspace_id)
                    .and_modify(|prev| {
                        prev.lsn = prev.lsn.max(batch.seqno);
                    })
                    .or_insert_with(|| EvictionWatermark {
                        keyspace: handle.clone(),
                        lsn: batch.seqno,
                    });

                handle.tree.clear().inspect_err(|e| {
                    log::error!("Keyspace clear failed during recovery: {e}");
                })?;
                if !touched_keyspaces
                    .iter()
                    .any(|touched: &&Keyspace| touched.id() == handle.id())
                {
                    touched_keyspaces.push(handle);
                }
            }

            recovery_budget.after_batch(
                db,
                touched_keyspaces.drain(..),
                keyspaces_lock.values(),
            )?;
        }

        log::debug!("Sealing recovered memtables");
        let mut recovered_count = 0;

        for wm in watermarks.values() {
            let tree = &wm.keyspace.tree;

            let keyspace_lsn = tree.get_highest_persisted_seqno();

            // IMPORTANT: Apply the sealed memtable only if it contains data newer than the
            // keyspace's persisted LSN.
            // Equality cannot prove that every item of the final batch was
            // flushed: recovery can be interrupted after persisting only one
            // chunk at this sequence number. Replaying an equal-LSN journal
            // is safe and preserves the items that were not flushed yet.
            let should_skip_sealed_memtable =
                keyspace_lsn.is_some_and(|keyspace_lsn| keyspace_lsn > wm.lsn);

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

                // The worker pool has not started yet. Keeping one sealed
                // memtable per old journal would rebuild an unbounded write
                // buffer before the workers can drain it.
                flush_recovery_chunk(db, &wm.keyspace, true)?;

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

    db.keyspace_id_counter
        .fetch_max(highest_encountered_keyspace_id + 1);

    log::trace!(
        "Set keyspace id counter to {}",
        db.keyspace_id_counter.get(),
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::flush_recovery_chunk;
    use crate::{Database, KeyspaceCreateOptions};
    use lsm_tree::AbstractTree;

    #[test]
    fn equal_seqno_later_table_wins_after_recovery_flush() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;
        {
            let db = Database::builder(folder.path())
                .worker_threads_unchecked(0)
                .open()?;
            let keyspace = db.keyspace("same-seqno", KeyspaceCreateOptions::default)?;
            let seqno = db.supervisor.seqno.next();
            keyspace.tree.insert("duplicate", "first", seqno);
            flush_recovery_chunk(&db, &keyspace, true)?;
            keyspace.tree.insert("duplicate", "last", seqno);
            flush_recovery_chunk(&db, &keyspace, true)?;
            assert_eq!(
                keyspace.get("duplicate")?.as_deref(),
                Some(b"last".as_slice())
            );
        }
        let db = Database::builder(folder.path())
            .worker_threads_unchecked(0)
            .open()?;
        let keyspace = db.keyspace("same-seqno", KeyspaceCreateOptions::default)?;
        assert_eq!(
            keyspace.get("duplicate")?.as_deref(),
            Some(b"last".as_slice())
        );
        Ok(())
    }

    #[test]
    fn partial_batch_flush_must_not_skip_remaining_sealed_items() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;
        {
            let db = Database::builder(folder.path())
                .worker_threads_unchecked(0)
                .open()?;
            let keyspace = db.keyspace("batch", KeyspaceCreateOptions::default)?;
            let mut batch = db.batch();
            batch.insert(&keyspace, "first", "one");
            batch.insert(&keyspace, "second", "two");
            batch.commit()?;
            db.supervisor.journal.get_writer()?.rotate()?;

            // Model a process killed after the first chunk of this batch was
            // flushed. The journal still contains both committed items.
            let batch_seqno = db.supervisor.seqno.get() - 1;
            keyspace.tree.clear_active_memtable();
            keyspace.tree.insert("first", "one", batch_seqno);
            flush_recovery_chunk(&db, &keyspace, true)?;
        }

        let db = Database::builder(folder.path())
            .worker_threads_unchecked(0)
            .open()?;
        let keyspace = db.keyspace("batch", KeyspaceCreateOptions::default)?;
        assert_eq!(keyspace.get("first")?.as_deref(), Some(b"one".as_slice()));
        assert_eq!(keyspace.get("second")?.as_deref(), Some(b"two".as_slice()));
        Ok(())
    }

    #[test]
    #[ignore = "manual crash-recovery stress test for a spilled sealed batch"]
    fn interrupted_spilled_sealed_batch_replays_every_item() -> crate::Result<()> {
        if let Some(path) = std::env::var_os("FJALL_SPILLED_INTERRUPT_CHILD") {
            let _db = Database::builder(path).worker_threads_unchecked(0).open()?;
            return Ok(());
        }

        let folder = tempfile::tempdir()?;
        let value = [b'i'; 256];
        const ROWS: u64 = 200_000;
        {
            let db = Database::builder(folder.path())
                .worker_threads_unchecked(0)
                .open()?;
            let keyspace = db.keyspace("spilled", KeyspaceCreateOptions::default)?;
            let mut batch = db.batch();
            batch.insert(&keyspace, "duplicate", "old");
            for i in 0..ROWS {
                batch.insert(&keyspace, i.to_be_bytes(), value);
            }
            batch.insert(&keyspace, "duplicate", "new");
            batch.commit()?;
            db.supervisor.journal.get_writer()?.rotate()?;
        }

        let mut child = std::process::Command::new(std::env::current_exe()?)
            .args([
                "--exact",
                "recovery::tests::interrupted_spilled_sealed_batch_replays_every_item",
                "--ignored",
            ])
            .env("FJALL_SPILLED_INTERRUPT_CHILD", folder.path())
            .spawn()?;
        let tables = folder.path().join("keyspaces/1/tables");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
        loop {
            if std::fs::read_dir(&tables)?.count() >= 2 {
                child.kill()?;
                child.wait()?;
                break;
            }
            if let Some(status) = child.try_wait()? {
                panic!("recovery child exited before interruption: {status}");
            }
            if std::time::Instant::now() >= deadline {
                child.kill()?;
                child.wait()?;
                panic!("recovery did not flush two chunks before deadline");
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        for _ in 0..2 {
            let db = Database::builder(folder.path())
                .worker_threads_unchecked(0)
                .open()?;
            let keyspace = db.keyspace("spilled", KeyspaceCreateOptions::default)?;
            assert_eq!(
                keyspace.get("duplicate")?.as_deref(),
                Some(b"new".as_slice())
            );
            for i in [0, ROWS / 2, ROWS - 1] {
                assert_eq!(
                    keyspace.get(i.to_be_bytes())?.as_deref(),
                    Some(value.as_slice())
                );
            }
        }
        Ok(())
    }

    #[test]
    fn oversized_sealed_journal_replays_in_chunks() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;
        let value = [b's'; 256];
        {
            let db = Database::builder(folder.path())
                .worker_threads_unchecked(0)
                .open()?;
            let keyspace = db.keyspace("sealed", || {
                KeyspaceCreateOptions::default().max_memtable_size(64 * 1024)
            })?;
            for i in 0_u64..8_000 {
                keyspace.insert(i.to_be_bytes(), value)?;
            }
            db.supervisor.journal.get_writer()?.rotate()?;
            keyspace.insert(0_u64.to_be_bytes(), b"new")?;
            keyspace.remove(4_000_u64.to_be_bytes())?;
        }

        for _ in 0..2 {
            let db = Database::builder(folder.path())
                .worker_threads_unchecked(0)
                .open()?;
            let keyspace = db.keyspace("sealed", KeyspaceCreateOptions::default)?;
            assert!(db.write_buffer_size() <= 128 * 1024);
            for i in [1_u64, 7_999] {
                assert_eq!(
                    keyspace.get(i.to_be_bytes())?.as_deref(),
                    Some(value.as_slice())
                );
            }
            assert_eq!(
                keyspace.get(0_u64.to_be_bytes())?.as_deref(),
                Some(b"new".as_slice())
            );
            assert!(keyspace.get(4_000_u64.to_be_bytes())?.is_none());
        }
        Ok(())
    }

    #[test]
    fn many_sealed_journals_do_not_accumulate_memtables() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;
        let value = [b'j'; 256];
        {
            let db = Database::builder(folder.path())
                .worker_threads_unchecked(0)
                .open()?;
            let keyspace = db.keyspace("sealed", || {
                KeyspaceCreateOptions::default().max_memtable_size(64 * 1024 * 1024)
            })?;
            for journal in 0_u64..3 {
                for i in 0_u64..20_000 {
                    keyspace.insert((journal * 20_000 + i).to_be_bytes(), value)?;
                }
                db.supervisor.journal.get_writer()?.rotate()?;
            }
        }

        let db = Database::builder(folder.path())
            .worker_threads_unchecked(0)
            .open()?;
        let keyspace = db.keyspace("sealed", KeyspaceCreateOptions::default)?;
        assert!(db.write_buffer_size() <= 1024);
        for i in [0_u64, 20_000, 59_999] {
            assert_eq!(
                keyspace.get(i.to_be_bytes())?.as_deref(),
                Some(value.as_slice())
            );
        }
        Ok(())
    }
}
