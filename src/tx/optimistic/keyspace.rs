// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::Keyspace;
use std::path::PathBuf;

/// Handle to a keyspace of a transactional database
#[derive(Clone)]
pub struct OptimisticTxKeyspace {
    pub(crate) inner: Keyspace,
}

impl AsRef<Keyspace> for OptimisticTxKeyspace {
    fn as_ref(&self) -> &Keyspace {
        self.inner()
    }
}

impl OptimisticTxKeyspace {
    /// Returns the underlying LSM-tree's path.
    #[must_use]
    pub fn path(&self) -> PathBuf {
        self.inner.path().into()
    }

    /// Approximates the number of items in the keyspace.
    ///
    /// For update- or delete-heavy workloads, this value will
    /// diverge from the real value, but is a O(1) operation.
    ///
    /// For insert-only workloads (e.g. logs, time series)
    /// this value is reliable.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{OptimisticTxDatabase, KeyspaceCreateOptions, Readable};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = OptimisticTxDatabase::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
    /// assert_eq!(tree.approximate_len(), 0);
    ///
    /// tree.insert("1", "abc")?;
    /// assert_eq!(tree.approximate_len(), 1);
    ///
    /// tree.remove("1")?;
    /// // Oops! approximate_len will not be reliable here
    /// assert_eq!(tree.approximate_len(), 2);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    #[must_use]
    pub fn approximate_len(&self) -> usize {
        self.inner.approximate_len()
    }

    /// Allows access to the inner keyspace handle, allowing to
    /// escape from the transactional context.
    #[doc(hidden)]
    #[must_use]
    pub fn inner(&self) -> &Keyspace {
        &self.inner
    }
}
