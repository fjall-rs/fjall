// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

/// Errors that can occur during journal recovery
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecoveryError {
    /// Batch had less items than expected, so it's incomplete
    InsufficientLength,

    /// Too many items in batch
    TooManyItems,

    /// The checksum value does not match the expected value
    ChecksumMismatch,

    /// An unparseable journal file name was encountered
    InvalidFileName,
}

impl std::fmt::Display for RecoveryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RecoveryError({self:?})")
    }
}

impl std::error::Error for RecoveryError {}
