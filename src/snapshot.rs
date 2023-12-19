use crate::{
    partition::Partition,
    prefix::Prefix,
    range::Range,
    value::{SeqNo, UserData, UserKey},
    Tree,
};
use std::ops::RangeBounds;

/// A snapshot captures a read-only point-in-time view of the tree at the time the snapshot was created.
///
/// As long as the snapshot is open, old versions of objects will not be evicted as to
/// keep the snapshot consistent. Thus, snapshots should only be kept around for as little as possible.
///
/// Snapshots do not persist across restarts.
#[derive(Clone)]
pub struct Snapshot {
    tree: Tree,
    root: PartitionSnapshot,
}

impl std::ops::Deref for Snapshot {
    type Target = PartitionSnapshot;

    fn deref(&self) -> &Self::Target {
        &self.root
    }
}

impl Snapshot {
    /// Creates a snapshot
    pub(crate) fn new(tree: Tree) -> Self {
        tree.open_snapshots
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);

        let seqno = tree.next_lsn.load(std::sync::atomic::Ordering::Acquire);

        log::debug!("Opening snapshot with seqno: {seqno}");

        let default_partition = tree.get_default_partition();

        Self {
            tree,
            root: PartitionSnapshot {
                partition_handle: default_partition,
                seqno,
            },
        }
    }

    /// Accesses a data partition.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn partition<P: AsRef<str>>(&self, name: P) -> crate::Result<PartitionSnapshot> {
        Ok(PartitionSnapshot {
            partition_handle: self.tree.partition(name)?,
            seqno: self.seqno,
        })
    }
}

impl Drop for Snapshot {
    fn drop(&mut self) {
        log::debug!("Closing snapshot");
        self.tree
            .open_snapshots
            .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
    }
}

/// Partition-scoped snapshot
#[allow(clippy::module_name_repetitions)]
#[derive(Clone)]
pub struct PartitionSnapshot {
    partition_handle: Partition,
    seqno: SeqNo,
}

impl PartitionSnapshot {
    /// Retrieves an item from the snapshot.
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// # use lsm_tree::{Config, Tree};
    /// #
    /// # let tree = Config::new(folder).open()?;
    /// let snapshot = tree.snapshot();
    ///
    /// tree.insert("a", "my_value")?;
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
    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> crate::Result<Option<UserData>> {
        Ok(self
            .partition_handle
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
    /// # use lsm_tree::{Config, Tree};
    /// #
    /// # let tree = Config::new(folder).open()?;
    /// #
    /// tree.insert("a", nanoid::nanoid!())?;
    /// tree.insert("f", nanoid::nanoid!())?;
    /// let snapshot = tree.snapshot();
    ///
    /// tree.insert("g", nanoid::nanoid!())?;
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
        self.partition_handle.create_iter(Some(self.seqno))
    }

    /// Returns an iterator over a range of items in the snapshot.
    ///
    /// Avoid using full or unbounded ranges as they may scan a lot of items (unless limited).
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// # use lsm_tree::{Config, Tree};
    /// #
    /// # let tree = Config::new(folder).open()?;
    /// #
    /// tree.insert("a", nanoid::nanoid!())?;
    /// let snapshot = tree.snapshot();
    ///
    /// tree.insert("f", nanoid::nanoid!())?;
    /// tree.insert("g", nanoid::nanoid!())?;
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
        self.partition_handle.create_range(range, Some(self.seqno))
    }

    /// Returns an iterator over a prefixed set of items in the snapshot.
    ///
    /// Avoid using an empty prefix as it may scan a lot of items (unless limited).
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// # use lsm_tree::{Config, Tree};
    /// #
    /// # let tree = Config::new(folder).open()?;
    /// #
    /// tree.insert("a", nanoid::nanoid!())?;
    /// tree.insert("ab", nanoid::nanoid!())?;
    /// let snapshot = tree.snapshot();
    ///
    /// tree.insert("abc", nanoid::nanoid!())?;
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
        self.partition_handle
            .create_prefix(prefix.as_ref(), Some(self.seqno))
    }

    /// Returns the first key-value pair in the snapshot.
    /// The key in this pair is the minimum key in the snapshot.
    ///
    /// # Examples
    ///
    /// ```
    /// # use lsm_tree::Error as TreeError;
    /// # use lsm_tree::{Tree, Config};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let tree = Config::new(folder).open()?;
    /// #
    /// tree.insert("5", "abc")?;
    /// tree.insert("3", "abc")?;
    /// let snapshot = tree.snapshot();
    ///
    /// tree.insert("1", "abc")?;
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
    pub fn first_key_value(&self) -> crate::Result<Option<(UserKey, UserData)>> {
        self.partition_handle
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
    /// # use lsm_tree::{Tree, Config};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let tree = Config::new(folder).open()?;
    /// #
    /// tree.insert("1", "abc")?;
    /// tree.insert("3", "abc")?;
    /// let snapshot = tree.snapshot();
    ///
    /// tree.insert("5", "abc")?;
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
    pub fn last_key_value(&self) -> crate::Result<Option<(UserKey, UserData)>> {
        self.partition_handle
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
    /// # use lsm_tree::{Config, Tree};
    /// #
    /// # let tree = Config::new(folder).open()?;
    /// let snapshot = tree.snapshot();
    ///
    /// assert!(!snapshot.contains_key("a")?);
    ///
    /// tree.insert("a", nanoid::nanoid!())?;
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
    /// # use lsm_tree::{Config, Tree};
    /// #
    /// # let tree = Config::new(folder).open()?;
    /// let snapshot = tree.snapshot();
    ///
    /// assert!(snapshot.is_empty()?);
    ///
    /// tree.insert("a", nanoid::nanoid!())?;
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
    /// if the snapshot is empty, use [`Self::is_empty`] instead.
    ///
    /// # Examples
    ///
    /// ```
    /// # use lsm_tree::Error as TreeError;
    /// # use lsm_tree::{Tree, Config};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let tree = Config::new(folder).open()?;
    /// let snapshot = tree.snapshot();
    ///
    /// assert_eq!(snapshot.len()?, 0);
    /// tree.insert("1", "abc")?;
    /// tree.insert("3", "abc")?;
    /// tree.insert("5", "abc")?;
    /// assert_eq!(snapshot.len()?, 0);
    /// #
    /// # Ok::<(), TreeError>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn len(&self) -> crate::Result<usize> {
        let mut count = 0;

        for item in &self.iter() {
            let _ = item?;
            count += 1;
        }

        Ok(count)
    }
}
