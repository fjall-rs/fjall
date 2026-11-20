// Copyright (c) 2026-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{file::link_or_copy, Backup, BackupId};
use crate::{
    file::{
        fsync_directory, rewrite_atomic, BLOBS_FOLDER, LOCK_FILE, TABLES_FOLDER, VERSION_MARKER,
    },
    file_resolver::KeyspaceFileResolver,
    locked_file::LockedFileGuard,
    manifest::{DbEditRecord, Manifest},
    version::FormatVersion,
};
use lsm_tree::FileResolver;
use std::path::Path;

impl Backup {
    /// Restores a backup into a new database folder, which can then be opened.
    ///
    /// The folder must be empty, or not exist yet.
    ///
    /// Tables and blob files are hard-linked if possible (they are never written to), otherwise copied.
    ///
    /// # IMPORTANT
    ///
    /// The restored database becomes the owner of the backup folder: only it may
    /// create backups in it from now on, the original database must never do so again.
    ///
    /// # Errors
    ///
    /// Returns an error if the backup doesn't exist, the folder is not empty,
    /// or an I/O error occurs.
    ///
    /// If the restore fails, the folder is left in an unusable state; remove it and try again.
    pub fn restore_backup(&self, id: BackupId, path: impl AsRef<Path>) -> crate::Result<()> {
        let path = path.as_ref();
        let backup_path = self.private_path(id);

        log::info!("Restoring backup #{id} into {}", path.display());

        if !self.backups.iter().any(|backup| backup.id == id) {
            return Err(crate::Error::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("backup #{id} does not exist"),
            )));
        }

        // NOTE: Fails early, before the lock file is created in someone else's folder
        Self::check_restore_folder_empty(path)?;

        std::fs::create_dir_all(path)?;

        // IMPORTANT: Held for the whole restore
        let _lock_file = LockedFileGuard::create_new(&path.join(LOCK_FILE))?;

        // NOTE: Again under the lock, another restore may have started in the meantime
        Self::check_restore_folder_empty(path)?;

        std::fs::create_dir_all(path.join(TABLES_FOLDER))?;
        std::fs::create_dir_all(path.join(BLOBS_FOLDER))?;

        // Copy manifest and WAL
        for entry in std::fs::read_dir(&backup_path)? {
            let entry = entry?;

            if entry.file_type()?.is_file() {
                let dst = path.join(entry.file_name());
                std::fs::copy(entry.path(), &dst)?;
                std::fs::File::open(&dst)?.sync_all()?;
            }
        }

        let has_wal = std::fs::read_dir(path)?.any(|entry| {
            entry.is_ok_and(|entry| {
                Path::new(&entry.file_name())
                    .extension()
                    .is_some_and(|extension| extension == "hwal")
            })
        });

        // NOTE: A backup taken with `BackupMode::Flush` has no WAL, but a database
        // needs an (active) WAL fragment to open
        if !has_wal {
            drop(hwal::Wal::builder().create_new(path)?);
        }

        // Copy tables and blob files
        let mut manifest = Manifest::open(path)?;
        let recovery = manifest.recover()?;

        for (&keyspace_id, version) in &recovery.keyspace_versions {
            let shared = self.shared_resolver(keyspace_id, recovery.legacy_watermark);

            let restored = KeyspaceFileResolver {
                db_path: path.to_path_buf(),
                keyspace_id,
                legacy: recovery.legacy_watermark,
            };

            for table in version.tables() {
                link_or_copy(
                    &shared.resolve_table(table.id()),
                    &restored.resolve_table(table.id()),
                )?;
            }

            for blob_file in version.blob_files() {
                link_or_copy(
                    &shared.resolve_blob_file(blob_file.id()),
                    &restored.resolve_blob_file(blob_file.id()),
                )?;
            }
        }

        // IMPORTANT: New files of the restored database must not collide with any file
        // in the backup folder (e.g. when restoring an older backup, or after a clear)
        let (next_table_id, next_blob_file_id) = self.next_file_ids();

        manifest.append_db_edits(&[DbEditRecord::SetFileIdCounters {
            tables: next_table_id,
            blob_files: next_blob_file_id,
        }])?;
        drop(manifest);

        fsync_directory(path.join(TABLES_FOLDER))?;
        fsync_directory(path.join(BLOBS_FOLDER))?;

        // Finally, write the version marker to signal completion
        let mut version_marker = vec![];
        FormatVersion::V4.write_file_header(&mut version_marker)?;
        rewrite_atomic(&path.join(VERSION_MARKER), &version_marker)?;

        fsync_directory(path)?;

        log::info!("Restored backup #{id} into {}", path.display());

        Ok(())
    }

    /// Fails if the restore folder has anything in it but a lock file.
    fn check_restore_folder_empty(path: &Path) -> crate::Result<()> {
        if !path.try_exists()? {
            return Ok(());
        }

        for entry in std::fs::read_dir(path)? {
            if entry?.file_name() != LOCK_FILE {
                return Err(crate::Error::Io(std::io::Error::new(
                    std::io::ErrorKind::AlreadyExists,
                    format!("restore folder {} is not empty", path.display()),
                )));
            }
        }

        Ok(())
    }

    /// Returns table and blob file IDs above every file of every backup in the folder
    /// (and above the counters the backups were taken with).
    fn next_file_ids(&self) -> (u64, u64) {
        self.backups.iter().fold((0, 0), |(tables, blobs), backup| {
            (
                tables.max(backup.next_table_id),
                blobs.max(backup.next_blob_file_id),
            )
        })
    }
}

#[cfg(test)]
mod tests;
