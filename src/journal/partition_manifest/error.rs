// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use std::num::ParseIntError;

#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),
    InvalidSeqno,
    Corrupted,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PartitionManifestError: {self:?}")
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(inner: std::io::Error) -> Self {
        Self::Io(inner)
    }
}

impl From<ParseIntError> for Error {
    fn from(_: ParseIntError) -> Self {
        Self::InvalidSeqno
    }
}
