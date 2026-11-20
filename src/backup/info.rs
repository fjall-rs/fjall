// Copyright (c) 2026-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::SeqNo;
use std::time::SystemTime;

/// Identifies a backup in a backup folder
pub type BackupId = u64;

/// One backup in a backup folder, see [`crate::Backup`]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackupInfo {
    /// ID of the backup
    pub id: BackupId,

    /// When the backup was created (committed)
    pub timestamp: SystemTime,

    /// Highest sequence number in the backup
    pub(crate) seqno: SeqNo,

    /// Size of the tables and blob files the backup references, in bytes
    ///
    /// NOTE: Files are shared between backups, so the sizes of several backups don't add up
    pub size_bytes: u64,

    /// Number of tables the backup references
    pub table_count: usize,

    /// Number of blob files the backup references
    pub blob_file_count: usize,
}

impl BackupInfo {
    /// Number of tables and blob files the backup references.
    #[must_use]
    pub fn file_count(&self) -> usize {
        self.table_count + self.blob_file_count
    }
}
