// Copyright (c) 2026-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{file::link_or_copy, Backup, BackupId, PRIVATE_FOLDER, SHARED_FOLDER};
use crate::{
    file::{fsync_directory, remove_dir_all_if_exists, BLOBS_FOLDER, TABLES_FOLDER},
    file_resolver::KeyspaceFileResolver,
    keyspace::InternalKeyspaceId,
    manifest::{DbEditRecord, Manifest, ManifestWriter},
    Database,
};
use lsm_tree::{AbstractTree, EditRecord, FileResolver};

/// What [`Backup::create_backup`] does with data that is not flushed yet
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackupMode {
    /// Flushes all keyspaces first, so the backup only consists of tables and blob files.
    ///
    /// Writes are blocked while the keyspaces are flushed.
    ///
    /// Restoring does not require replaying the WAL.
    Flush,

    /// Copies the WAL fragments with the unflushed data into the backup.
    ///
    /// Writes are only blocked while the WAL is sealed.
    ///
    /// Restoring requires replaying the WAL.
    KeepLogs,
}

/// A table or blob file the backup references
enum BackupFile {
    Table(InternalKeyspaceId, lsm_tree::TableId),
    BlobFile(InternalKeyspaceId, u64),
}

/// Writes the manifest snapshot into the backup's manifest,
/// and records the files it references, so they can be copied after the cut
struct FileCollector<'a> {
    inner: &'a mut dyn ManifestWriter,
    files: &'a mut Vec<BackupFile>,
}

impl ManifestWriter for FileCollector<'_> {
    fn append_db_edits(&mut self, records: &[DbEditRecord]) -> crate::Result<()> {
        self.inner.append_db_edits(records)
    }

    fn append_version_edit(
        &mut self,
        keyspace_id: InternalKeyspaceId,
        version_edit: &lsm_tree::VersionEdit,
    ) -> crate::Result<()> {
        for record in version_edit.iter() {
            match record {
                EditRecord::AddTables { tables, .. } => {
                    self.files.extend(
                        tables
                            .iter()
                            .map(|table| BackupFile::Table(keyspace_id, table.id())),
                    );
                }
                EditRecord::AddBlobFiles { blob_files } => {
                    self.files.extend(
                        blob_files
                            .iter()
                            .map(|blob_file| BackupFile::BlobFile(keyspace_id, blob_file.id())),
                    );
                }
                _ => {}
            }
        }

        self.inner.append_version_edit(keyspace_id, version_edit)
    }
}

impl Backup {
    /// Creates a new backup of the database.
    ///
    /// The backup is incremental: tables and blob files the
    /// folder already has (from earlier backups) are not copied again.
    ///
    /// Writes are only shortly blocked while the WAL is sealed, which is O(1) for a database.
    ///
    /// Files are hard-linked if possible (same filesystem), otherwise copied.
    ///
    /// # Errors
    ///
    /// Returns an error if an I/O error occurs, or the database is poisoned.
    pub fn create_backup(&mut self, db: &Database, mode: BackupMode) -> crate::Result<BackupId> {
        let flush_first = mode == BackupMode::Flush;

        let id = self.backups.last().map_or(1, |backup| backup.id + 1);
        let backup_path = self.private_path(id);

        log::info!("Creating backup #{id} at {}", backup_path.display());

        // NOTE: Leftover of a backup that failed earlier, it was never committed
        remove_dir_all_if_exists(&backup_path)?;
        std::fs::create_dir_all(&backup_path)?;

        // IMPORTANT: Only one backup of the database at a time (e.g. from two backup folders),
        // because the WAL can only be pinned once
        //
        // Lock order: backup lock, then everything else
        let _backup_lock = db
            .supervisor
            .backup_lock
            .lock()
            .map_err(|_| crate::Error::Poisoned)?;

        let mut manifest = Manifest::create_new(&backup_path)?;
        let mut files = Vec::new();

        // Rotate active WAL (and possibly flush)
        let (wal_pin, wal_fragments, snapshot) = {
            let mut ledger = db.supervisor.ledger()?;

            // IMPORTANT: Check the poisoned flag after getting the ledger lock, otherwise TOCTOU
            if db.is_poisoned.is_poisoned() {
                return Err(crate::Error::Poisoned);
            }

            {
                // NOTE: Lock order: ledger lock, keyspaces lock, flush lock
                let keyspaces = db
                    .supervisor
                    .keyspaces
                    .read()
                    .map_err(|_| crate::Error::Poisoned)?;

                // NOTE: Seals (and syncs) the active fragment, so every fragment
                // the backup needs is never written to again
                ledger.wal.rotate(&keyspaces)?;

                // NOTE: Flushes every item in the (now sealed) fragments, like `Database::flush_all`
                if flush_first {
                    for keyspace in keyspaces.values() {
                        let _flush_lock = keyspace.tree.get_flush_lock();
                        keyspace.flush_locked(&mut ledger)?;
                    }
                }
            }

            // NOTE: With `BackupMode::Flush`, every item is in a table, so no WAL is needed
            let (wal_pin, wal_fragments) = if flush_first {
                // NOTE: After the keyspaces lock is released again
                db.supervisor.prune_wal(&mut ledger)?;

                (None, Vec::new())
            } else {
                (
                    Some(ledger.wal.pin()),
                    ledger.wal.sealed_fragment_paths().collect::<Vec<_>>(),
                )
            };

            // NOTE: Keeps the version at the cut (and so all its files) alive until they are copied
            let snapshot = db.supervisor.snapshot_tracker.open();

            // NOTE: Takes the keyspaces lock itself
            manifest.write_snapshot(|writer| {
                db.supervisor.write_manifest_snapshot(&mut FileCollector {
                    inner: writer,
                    files: &mut files,
                })
            })?;

            (wal_pin, wal_fragments, snapshot)
        };

        // Copy WAL
        for src in &wal_fragments {
            let dst = backup_path.join(src.file_name().unwrap_or_default());
            link_or_copy(src, &dst)?;
        }

        // Drop the WAL guard to allow writes again
        if let Some(wal_pin) = wal_pin {
            drop(wal_pin);

            // NOTE: Flushes skipped pruning while the WAL was pinned, so catch up
            db.supervisor.prune_wal(&mut db.supervisor.ledger()?)?;
        }

        // Copy data files (tables + blob files)
        let legacy = db.supervisor.legacy_watermark.get().copied();

        let db_resolver = |keyspace_id| KeyspaceFileResolver {
            db_path: db.path().to_path_buf(),
            keyspace_id,
            legacy,
        };

        for file in &files {
            let (src, dst) = match *file {
                BackupFile::Table(keyspace_id, id) => (
                    db_resolver(keyspace_id).resolve_table(id),
                    self.shared_resolver(keyspace_id, legacy).resolve_table(id),
                ),
                BackupFile::BlobFile(keyspace_id, id) => (
                    db_resolver(keyspace_id).resolve_blob_file(id),
                    self.shared_resolver(keyspace_id, legacy)
                        .resolve_blob_file(id),
                ),
            };

            // NOTE: Same name = same file, because only one database feeds the folder
            if dst.try_exists()? {
                continue;
            }

            link_or_copy(&src, &dst)?;
        }
        drop(snapshot);

        // Sync all stuff
        fsync_directory(self.path.join(SHARED_FOLDER).join(TABLES_FOLDER))?;
        fsync_directory(self.path.join(SHARED_FOLDER).join(BLOBS_FOLDER))?;
        fsync_directory(&backup_path)?;

        // Place current marker, this makes a backup "finalized"
        manifest.place_current_marker()?;
        fsync_directory(&backup_path)?;
        fsync_directory(self.path.join(PRIVATE_FOLDER))?;

        drop(manifest);

        // Epilogue
        let (info, files) = self.read_backup(id)?;
        log::info!("Created backup #{id}: {info:?}");
        self.files.insert(id, files);
        self.backups.push(info);

        Ok(id)
    }
}

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests;
