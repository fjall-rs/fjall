// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

pub(crate) mod batch_reader;
pub(crate) mod reader;

use super::writer::Writer as JournalWriter;
use std::path::Path;

// TODO: 2.0.0 move enums into batch_reader file

/// Recovery mode to use
///
/// Based on `RocksDB`'s WAL Recovery Modes: <https://github.com/facebook/rocksdb/wiki/WAL-Recovery-Modes>
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum RecoveryMode {
    /// The last batch in the journal may be corrupt on crash,
    /// and will be discarded without error.
    ///
    /// This mode will error on any other IO or consistency error, so
    /// any data up to the tail will be consistent.
    ///
    /// This is the default mode.
    #[default]
    TolerateCorruptTail,
    // TODO: in the future?
    /*  /// Skips corrupt (invalid checksum) batches. This may violate
    /// consistency, but will recover as much data as possible.
    SkipInvalidBatches, */
    // TODO: absolute consistency
}

/// Errors that can occur during journal recovery
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecoveryError {
    /// Batch had less items than expected, so it's incomplete
    InsufficientLength,

    /* /// Batch was not terminated, so it's possibly incomplete
    MissingTerminator, */
    /// Too many items in batch
    TooManyItems,

    /// The checksum value does not match the expected value
    ChecksumMismatch,
}

// TODO: don't require locking for sync check
#[allow(clippy::module_name_repetitions)]
pub struct JournalShard {
    pub(crate) writer: JournalWriter,
    pub(crate) should_sync: bool,
}

impl JournalShard {
    pub fn rotate<P: AsRef<Path>>(&mut self, path: P) -> crate::Result<()> {
        self.should_sync = false;
        self.writer.rotate(path)
    }

    pub fn create_new<P: AsRef<Path>>(path: P) -> crate::Result<Self> {
        Ok(Self {
            writer: JournalWriter::create_new(path)?,
            should_sync: bool::default(),
        })
    }

    pub fn from_file<P: AsRef<Path>>(path: P) -> crate::Result<Self> {
        Ok(Self {
            writer: JournalWriter::from_file(path)?,
            should_sync: bool::default(),
        })
    }
}
