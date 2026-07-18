// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use lsm_tree::{Guard as _Guard, UserKey, UserValue};

/// Guard to access key-value pairs
pub struct Guard(pub(crate) lsm_tree::IterGuardImpl);

impl Guard {
    // TODO: is_ok?

    /// Accesses the key-value pair if the predicate returns `true`.
    ///
    /// The predicate receives the key - if returning `false`, the value
    /// may not be loaded if the tree is key-value separated.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Database, KeyspaceCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = Database::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
    /// tree.insert("abc", "my_value")?;
    ///
    /// let (k,v) = tree.prefix("a")
    ///     .next()
    ///     .unwrap()
    ///     .into_inner_if(|key| key.starts_with(b"a"))?;
    ///
    /// assert_eq!(b"abc", &*k);
    /// assert_eq!(Some(b"my_value".as_slice()), v.as_deref());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn into_inner_if(
        self,
        pred: impl Fn(&crate::UserKey) -> bool,
    ) -> crate::Result<(UserKey, Option<UserValue>)> {
        self.0.into_inner_if(pred).map_err(Into::into)
    }

    /// Returns the key-value tuple.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Database, KeyspaceCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = Database::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
    /// tree.insert("a", "my_value")?;
    ///
    /// let (k,v) = tree.prefix("a")
    ///     .next()
    ///     .unwrap()
    ///     .into_inner()?;
    ///
    /// assert_eq!(b"a", &*k);
    /// assert_eq!(b"my_value", &*v);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn into_inner(self) -> crate::Result<crate::KvPair> {
        self.0.into_inner().map_err(Into::into)
    }

    /// Returns the key.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Database, KeyspaceCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = Database::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
    /// tree.insert("a", "my_value")?;
    ///
    /// let item = tree.prefix("a").next().unwrap().key()?;
    /// assert_eq!(b"a", &*item);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn key(self) -> crate::Result<crate::UserKey> {
        self.0.key().map_err(Into::into)
    }

    /// Returns the value size.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Database, KeyspaceCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = Database::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
    /// tree.insert("a", "my_value")?;
    ///
    /// let item = tree.prefix("a").next().unwrap().size()?;
    /// assert_eq!(8, item);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn size(self) -> crate::Result<u32> {
        self.0.size().map_err(Into::into)
    }

    /// Returns the value.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Database, KeyspaceCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = Database::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
    /// tree.insert("a", "my_value")?;
    ///
    /// let item = tree.prefix("a").next().unwrap().value()?;
    /// assert_eq!(b"my_value", &*item);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn value(self) -> crate::Result<crate::UserValue> {
        self.0.value().map_err(Into::into)
    }
}
