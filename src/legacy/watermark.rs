// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

/// Separates migrated v3 files from files written by v4.
///
/// Table and blob file IDs come from DB-wide counters, which are raised above all
/// v3 IDs when the v3 trees are opened.
///
/// So every v3 file has an ID below the watermark,
/// and every file written afterwards has an ID at or above it.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct LegacyWatermark {
    /// Table IDs below this are v3 tables
    pub tables: u64,

    /// Blob file IDs below this are v3 blob files
    pub blob_files: u64,
}
