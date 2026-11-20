// Copyright (c) 2026-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{Backup, BackupId, PRIVATE_FOLDER};
use crate::{file::fsync_directory, manifest::CURRENT_VERSION_MARKER};

impl Backup {
    /// Removes a backup.
    ///
    /// # Errors
    ///
    /// Returns an error if the backup doesn't exist, or an I/O error occurs.
    pub fn remove_backup(&mut self, id: BackupId) -> crate::Result<()> {
        self.inner_remove_backup(id)?;
        self.remove_unreferenced_files()
    }

    /// Removes all but the newest `keep` backups (by ID).
    ///
    /// # Errors
    ///
    /// Returns an error if an I/O error occurs.
    pub fn prune_old_backups(&mut self, keep: usize) -> crate::Result<()> {
        let prune_count = self.backups.len().saturating_sub(keep);

        let ids = self
            .backups
            .iter()
            .take(prune_count)
            .map(|backup| backup.id)
            .collect::<Vec<_>>();

        for id in ids {
            self.inner_remove_backup(id)?;
        }

        self.remove_unreferenced_files()
    }

    /// Removes a backup's private folder (not its shared files).
    fn inner_remove_backup(&mut self, id: BackupId) -> crate::Result<()> {
        let Some(idx) = self.backups.iter().position(|backup| backup.id == id) else {
            return Err(crate::Error::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("backup #{id} does not exist"),
            )));
        };

        log::info!("Removing backup #{id}");

        let backup_path = self.private_path(id);

        // IMPORTANT: Remove "current" first, that makes the backup non-finalized, thus eligible to be removed
        std::fs::remove_file(backup_path.join(CURRENT_VERSION_MARKER))?;
        fsync_directory(&backup_path)?;

        self.backups.remove(idx);
        self.files.remove(&id);

        std::fs::remove_dir_all(&backup_path)?;
        fsync_directory(self.path.join(PRIVATE_FOLDER))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests;
