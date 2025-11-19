use std::ops::RangeBounds;

use crate::{Guard, Keyspace};
use lsm_tree::{KvPair, UserValue};

/// Readable snapshot
///
/// Can be used to pass a write transaction into a function that expects a snapshot.
///
/// # Example
///
/// TODO: example
pub trait Readable {
    /// Retrieves an item from the transaction's state.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Database, KeyspaceCreateOptions, Readable};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = Database::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
    /// tree.insert("a", "my_value")?;
    ///
    /// let snapshot = db.snapshot();
    /// let item = snapshot.get(&tree, "a")?;
    /// assert_eq!(Some("my_value".as_bytes().into()), item);
    ///
    /// tree.insert("b", "my_updated_value")?;
    ///
    /// // Repeatable read
    /// let item = snapshot.get(&tree, "a")?;
    /// assert_eq!(Some("my_value".as_bytes().into()), item);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    fn get<K: AsRef<[u8]>>(
        &self,
        keyspace: impl AsRef<Keyspace>,
        key: K,
    ) -> crate::Result<Option<UserValue>>;

    /// Returns `true` if the transaction's state contains the specified key.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Database, KeyspaceCreateOptions, Readable};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = Database::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
    /// tree.insert("a", "my_value")?;
    ///
    /// let snapshot = db.snapshot();
    /// assert!(snapshot.contains_key(&tree, "a")?);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    fn contains_key<K: AsRef<[u8]>>(
        &self,
        keyspace: impl AsRef<Keyspace>,
        key: K,
    ) -> crate::Result<bool>;

    /// Returns the first key-value pair in the transaction's state.
    /// The key in this pair is the minimum key in the transaction's state.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Database, KeyspaceCreateOptions, Readable};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = Database::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
    /// tree.insert("1", "abc")?;
    /// tree.insert("3", "abc")?;
    /// tree.insert("5", "abc")?;
    ///
    /// let (key, _) = db.snapshot().first_key_value(&tree)?.expect("item should exist");
    /// assert_eq!(&*key, "1".as_bytes());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    fn first_key_value(&self, keyspace: impl AsRef<Keyspace>) -> crate::Result<Option<KvPair>>;

    /// Returns the last key-value pair in the transaction's state.
    /// The key in this pair is the maximum key in the transaction's state.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Database, KeyspaceCreateOptions, Readable};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = Database::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
    /// tree.insert("1", "abc")?;
    /// tree.insert("3", "abc")?;
    /// tree.insert("5", "abc")?;
    ///
    /// let (key, _) = db.snapshot().last_key_value(&tree)?.expect("item should exist");
    /// assert_eq!(&*key, "5".as_bytes());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    fn last_key_value(&self, keyspace: impl AsRef<Keyspace>) -> crate::Result<Option<KvPair>>;

    /// Retrieves the size of an item from the transaction's state.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Database, KeyspaceCreateOptions, Readable};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = Database::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
    /// tree.insert("a", "my_value")?;
    ///
    /// let snapshot = db.snapshot();
    /// let item = snapshot.size_of(&tree, "a")?.unwrap_or_default();
    /// assert_eq!("my_value".len() as u32, item);
    ///
    /// tree.insert("b", "my_updated_value")?;
    ///
    /// // Repeatable read
    /// let item = snapshot.size_of(&tree, "a")?.unwrap_or_default();
    /// assert_eq!("my_value".len() as u32, item);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    fn size_of<K: AsRef<[u8]>>(
        &self,
        keyspace: impl AsRef<Keyspace>,
        key: K,
    ) -> crate::Result<Option<u32>>;

    /// Returns `true` if the parkeyspacetition is empty.
    ///
    /// This operation has O(log N) complexity.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Database, KeyspaceCreateOptions, Readable};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = Database::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
    /// assert!(db.snapshot().is_empty(&tree)?);
    ///
    /// tree.insert("a", "abc")?;
    /// assert!(!db.snapshot().is_empty(&tree)?);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    fn is_empty(&self, keyspace: impl AsRef<Keyspace>) -> crate::Result<bool> {
        self.first_key_value(keyspace).map(|x| x.is_none())
    }

    /// Iterates over the transaction's state.
    ///
    /// Avoid using this function, or limit it as otherwise it may scan a lot of items.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Database, KeyspaceCreateOptions, Readable};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = Database::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
    /// tree.insert("a", "abc")?;
    /// tree.insert("f", "abc")?;
    /// tree.insert("g", "abc")?;
    ///
    /// assert_eq!(3, db.snapshot().iter(&tree).count());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    fn iter(
        &self,
        keyspace: impl AsRef<Keyspace>,
    ) -> impl DoubleEndedIterator<Item = Guard> + 'static;

    /// Scans the entire keyspace, returning the amount of items.
    ///
    /// ###### Caution
    ///
    /// This operation scans the entire keyspace: O(n) complexity!
    ///
    /// Never, under any circumstances, use .`len()` == 0 to check
    /// if the keyspace is empty, use [`ReadTransaction::is_empty`] instead.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Database, KeyspaceCreateOptions, Readable};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = Database::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
    /// tree.insert("a", "my_value")?;
    /// tree.insert("b", "my_value2")?;
    ///
    /// let snapshot = db.snapshot();
    /// assert_eq!(2, snapshot.len(&tree)?);
    ///
    /// tree.insert("c", "my_value3")?;
    ///
    /// // Repeatable read
    /// assert_eq!(2, snapshot.len(&tree)?);
    ///
    /// // Start new snapshot
    /// let snapshot = db.snapshot();
    /// assert_eq!(3, snapshot.len(&tree)?);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    fn len(&self, keyspace: impl AsRef<Keyspace>) -> crate::Result<usize> {
        let mut count = 0;

        for guard in self.iter(keyspace) {
            let _ = guard.key()?;
            count += 1;
        }

        Ok(count)
    }

    /// Iterates over a range of the transaction's state.
    ///
    /// Avoid using full or unbounded ranges as they may scan a lot of items (unless limited).
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Database, KeyspaceCreateOptions, Readable};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = Database::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
    /// tree.insert("a", "abc")?;
    /// tree.insert("f", "abc")?;
    /// tree.insert("g", "abc")?;
    ///
    /// assert_eq!(2, db.snapshot().range(&tree, "a"..="f").count());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    fn range<K: AsRef<[u8]>, R: RangeBounds<K>>(
        &self,
        keyspace: impl AsRef<Keyspace>,
        range: R,
    ) -> impl DoubleEndedIterator<Item = Guard> + 'static;

    /// Iterates over a prefixed set of the transaction's state.
    ///
    /// Avoid using an empty prefix as it may scan a lot of items (unless limited).
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Database, KeyspaceCreateOptions, Readable};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = Database::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
    /// tree.insert("a", "abc")?;
    /// tree.insert("ab", "abc")?;
    /// tree.insert("abc", "abc")?;
    ///
    /// assert_eq!(2, db.snapshot().prefix(&tree, "ab").count());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    fn prefix<K: AsRef<[u8]>>(
        &self,
        keyspace: impl AsRef<Keyspace>,
        prefix: K,
    ) -> impl DoubleEndedIterator<Item = Guard> + 'static;
}
