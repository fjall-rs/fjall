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

    /// The last batch in the journal may be corrupt on crash, and
    /// will be discarded without error, like in [`RecoveryMode::TolerateCorruptTail`].
    ///
    /// An unreadable region that a **completed** batch follows, however, is not a
    /// torn tail: something was written after it, so recovering would silently drop
    /// data that was acknowledged. This mode reports those as
    /// [`RecoveryError::MidJournalCorruption`] instead, and leaves the file alone.
    ///
    /// Use this when the journal is a log another system replicates or otherwise
    /// depends on, where losing entries without noticing breaks the guarantees of
    /// whatever is on top of it.
    Strict,
    // TODO: in the future?
    /*  /// Skips corrupt (invalid checksum) batches. This may violate
    /// consistency, but will recover as much data as possible.
    SkipInvalidBatches, */
    // TODO: absolute consistency
}

/// Errors that can occur during journal recovery
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
#[expect(clippy::module_name_repetitions)]
pub enum RecoveryError {
    /// Batch had less items than expected, so it's incomplete
    InsufficientLength,

    /* /// Batch was not terminated, so it's possibly incomplete
    MissingTerminator, */
    /// Too many items in batch
    TooManyItems,

    /// The checksum value does not match the expected value
    ChecksumMismatch,

    /// A completed batch exists after an unreadable region, so the corruption is
    /// in the middle of the journal rather than at a torn tail.
    ///
    /// Nothing was truncated: the journal is left as it is for the caller to
    /// inspect or repair. Only reported in [`RecoveryMode::Strict`].
    MidJournalCorruption,

    /// An unparseable journal file name was encountered
    InvalidFileName,
}

impl std::fmt::Display for RecoveryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RecoveryError({self:?})")
    }
}

impl std::error::Error for RecoveryError {}
