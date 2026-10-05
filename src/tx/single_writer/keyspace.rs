// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::Keyspace;
use std::path::PathBuf;

/// Handle to a keyspace of a transactional database
#[derive(Clone)]
pub struct SingleWriterTxKeyspace {
    pub(crate) inner: Keyspace,
}

impl AsRef<Keyspace> for SingleWriterTxKeyspace {
    fn as_ref(&self) -> &Keyspace {
        self.inner()
    }
}

impl SingleWriterTxKeyspace {
    /// Returns the underlying LSM-tree's path.
    #[must_use]
    pub fn path(&self) -> PathBuf {
        self.inner.path().into()
    }

    /// Allows access to the inner keyspace handle, allowing to
    /// escape from the transactional context.
    #[doc(hidden)]
    #[must_use]
    pub fn inner(&self) -> &Keyspace {
        &self.inner
    }
}
