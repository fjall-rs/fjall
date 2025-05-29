// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

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
#[allow(clippy::module_name_repetitions)]
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

impl std::fmt::Display for RecoveryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RecoveryError({self:?})")
    }
}

impl std::error::Error for RecoveryError {}
