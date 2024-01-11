use crate::{
    prefix::Prefix,
    range::Range,
    value::{SeqNo, UserKey, UserValue},
    Tree,
};
use std::{
    ops::RangeBounds,
    sync::{
        atomic::{self, AtomicU32},
        Arc,
    },
};

#[derive(Clone, Debug, Default)]
pub struct SnapshotCounter(Arc<AtomicU32>);

impl std::ops::Deref for SnapshotCounter {
    type Target = Arc<AtomicU32>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl SnapshotCounter {
    pub fn increment(&self) -> u32 {
        self.fetch_add(1, atomic::Ordering::Release)
    }

    pub fn decrement(&self) -> u32 {
        self.fetch_sub(1, atomic::Ordering::Release)
    }

    pub fn has_open_snapshots(&self) -> bool {
        self.load(atomic::Ordering::Acquire) > 0
    }
}

/// A snapshot captures a read-only point-in-time view of the tree at the time the snapshot was created
///
/// As long as the snapshot is open, old versions of objects will not be evicted as to
/// keep the snapshot consistent. Thus, snapshots should only be kept around for as little as possible.
///
/// Snapshots do not persist across restarts.
#[derive(Clone)]
pub struct Snapshot {
    tree: Tree,
    seqno: SeqNo,
}

impl Snapshot {
    /// Creates a snapshot
    pub(crate) fn new(tree: Tree, seqno: SeqNo) -> Self {
        tree.open_snapshots.increment();
        log::debug!("Opening snapshot with seqno: {seqno}");
        Self { tree, seqno }
    }

    /// Retrieves an item from the snapshot.
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// use lsm_tree::{Config, Tree};
    ///
    /// let tree = Config::new(folder).open()?;
    /// let snapshot = tree.snapshot(0);
    ///
    /// tree.insert("a", "my_value", 0);
    ///
    /// let item = snapshot.get("a")?;
    /// assert_eq!(None, item);
    /// #
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> crate::Result<Option<UserValue>> {
        Ok(self
            .tree
            .get_internal_entry(key, true, Some(self.seqno))?
            .map(|x| x.value))
    }

    #[allow(clippy::iter_not_returning_iterator)]
    /// Returns an iterator that scans through the entire snapshot.
    ///
    /// Avoid using this function, or limit it as otherwise it may scan a lot of items.
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// use lsm_tree::{Config, Tree};
    ///
    /// let tree = Config::new(folder).open()?;
    ///
    /// tree.insert("a", "abc", 0);
    /// tree.insert("f", "abc", 1);
    /// let snapshot = tree.snapshot(2);
    ///
    /// tree.insert("g", "abc", 2);
    ///
    /// assert_eq!(2, snapshot.iter().into_iter().count());
    /// #
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    #[must_use]
    pub fn iter(&self) -> Range {
        self.tree.create_iter(Some(self.seqno))
    }

    /// Returns an iterator over a range of items in the snapshot.
    ///
    /// Avoid using full or unbounded ranges as they may scan a lot of items (unless limited).
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// use lsm_tree::{Config, Tree};
    ///
    /// let tree = Config::new(folder).open()?;
    ///
    /// tree.insert("a", "abc", 0);
    /// let snapshot = tree.snapshot(1);
    ///
    /// tree.insert("f", "abc", 1);
    /// tree.insert("g", "abc", 2);
    ///
    /// assert_eq!(1, snapshot.range("a"..="f").into_iter().count());
    /// #
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn range<K: AsRef<[u8]>, R: RangeBounds<K>>(&self, range: R) -> Range {
        self.tree.create_range(range, Some(self.seqno))
    }

    /// Returns an iterator over a prefixed set of items in the snapshot.
    ///
    /// Avoid using an empty prefix as it may scan a lot of items (unless limited).
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// use lsm_tree::{Config, Tree};
    ///
    /// let tree = Config::new(folder).open()?;
    ///
    /// tree.insert("a", "abc", 0);
    /// tree.insert("ab", "abc", 1);
    /// let snapshot = tree.snapshot(2);
    ///
    /// tree.insert("abc", "abc", 2);
    ///
    /// assert_eq!(2, snapshot.prefix("a").into_iter().count());
    /// #
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn prefix<K: AsRef<[u8]>>(&self, prefix: K) -> Prefix {
        self.tree.create_prefix(prefix.as_ref(), Some(self.seqno))
    }

    /// Returns the first key-value pair in the snapshot.
    /// The key in this pair is the minimum key in the snapshot.
    ///
    /// # Examples
    ///
    /// ```
    /// # use lsm_tree::Error as TreeError;
    /// use lsm_tree::{Tree, Config};
    ///
    /// # let folder = tempfile::tempdir()?;
    /// let tree = Config::new(folder).open()?;
    ///
    /// tree.insert("5", "abc", 0);
    /// tree.insert("3", "abc", 1);
    /// let snapshot = tree.snapshot(2);
    ///
    /// tree.insert("1", "abc", 2);
    ///
    /// let (key, _) = snapshot.first_key_value()?.expect("item should exist");
    /// assert_eq!(&*key, "3".as_bytes());
    /// #
    /// # Ok::<(), TreeError>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn first_key_value(&self) -> crate::Result<Option<(UserKey, UserValue)>> {
        self.tree
            .create_iter(Some(self.seqno))
            .into_iter()
            .next()
            .transpose()
    }

    /// Returns the las key-value pair in the snapshot.
    /// The key in this pair is the maximum key in the snapshot.
    ///
    /// # Examples
    ///
    /// ```
    /// # use lsm_tree::Error as TreeError;
    /// use lsm_tree::{Tree, Config};
    ///
    /// # let folder = tempfile::tempdir()?;
    /// let tree = Config::new(folder).open()?;
    ///
    /// tree.insert("1", "abc", 0);
    /// tree.insert("3", "abc", 1);
    /// let snapshot = tree.snapshot(2);
    ///
    /// tree.insert("5", "abc", 2);
    ///
    /// let (key, _) = snapshot.last_key_value()?.expect("item should exist");
    /// assert_eq!(&*key, "3".as_bytes());
    /// #
    /// # Ok::<(), TreeError>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn last_key_value(&self) -> crate::Result<Option<(UserKey, UserValue)>> {
        self.tree
            .create_iter(Some(self.seqno))
            .into_iter()
            .next_back()
            .transpose()
    }

    /// Returns `true` if the snapshot contains the specified key.
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// use lsm_tree::{Config, Tree};
    ///
    /// let tree = Config::new(folder).open()?;
    /// let snapshot = tree.snapshot(0);
    ///
    /// assert!(!snapshot.contains_key("a")?);
    ///
    /// tree.insert("a", "abc", 0);
    /// assert!(!snapshot.contains_key("a")?);
    /// #
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn contains_key<K: AsRef<[u8]>>(&self, key: K) -> crate::Result<bool> {
        self.get(key).map(|x| x.is_some())
    }

    /// Returns `true` if the snapshot is empty.
    ///
    /// This operation has O(1) complexity.
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// use lsm_tree::{Config, Tree};
    ///
    /// let tree = Config::new(folder).open()?;
    /// let snapshot = tree.snapshot(0);
    ///
    /// assert!(snapshot.is_empty()?);
    ///
    /// tree.insert("a", "abc", 0);
    /// assert!(snapshot.is_empty()?);
    /// #
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn is_empty(&self) -> crate::Result<bool> {
        self.first_key_value().map(|x| x.is_none())
    }

    /// Scans the entire snapshot, returning the amount of items.
    ///
    /// ###### Caution
    ///
    /// This operation scans the entire tree: O(n) complexity!
    ///
    /// Never, under any circumstances, use .len() == 0 to check
    /// if the snapshot is empty, use [`Snapshot::is_empty`] instead.
    ///
    /// # Examples
    ///
    /// ```
    /// # use lsm_tree::Error as TreeError;
    /// use lsm_tree::{Tree, Config};
    ///
    /// # let folder = tempfile::tempdir()?;
    /// let tree = Config::new(folder).open()?;
    /// let snapshot = tree.snapshot(0);
    ///
    /// assert_eq!(snapshot.len()?, 0);
    /// tree.insert("1", "abc", 0);
    /// tree.insert("3", "abc", 1);
    /// tree.insert("5", "abc", 2);
    /// assert_eq!(snapshot.len()?, 0);
    /// #
    /// # Ok::<(), TreeError>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn len(&self) -> crate::Result<usize> {
        // TODO: shouldn't use block cache
        Ok(self.iter().into_iter().filter(Result::is_ok).count())
    }
}

impl Drop for Snapshot {
    fn drop(&mut self) {
        log::debug!("Closing snapshot");
        self.tree.open_snapshots.decrement();
    }
}
