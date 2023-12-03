use std::ops::RangeBounds;

use crate::{prefix::Prefix, range::Range, value::SeqNo, Tree};

#[derive(Clone)]
/// A snapshot captures a read-only point-in-time view of the tree at the time the snapshot was created.
///
/// As long as the snapshot is open, old versions of objects will not be evicted
/// as to keep the snapshot consistent.
///
/// Thus, snapshots should only be kept around for as little as possible.
///
/// Snapshots do not persist across restarts.
pub struct Snapshot {
    tree: Tree,
    seqno: SeqNo,
}

impl Snapshot {
    /// Creates a snapshot
    pub(crate) fn new(tree: Tree, seqno: SeqNo) -> Self {
        log::debug!("Opening snapshot with seqno: {seqno}");
        Self { tree, seqno }
    }

    /// Retrieves an item from the snapshot.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> crate::Result<Option<Vec<u8>>> {
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
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn iter(&self) -> crate::Result<Range<'_>> {
        self.tree.create_iter(Some(self.seqno))
    }

    /// Returns an iterator over a range of items in the snapshot.
    ///
    /// Avoid using full or unbounded ranges as they may scan a lot of items (unless limited).
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn range<K: AsRef<[u8]>, R: RangeBounds<K>>(&self, range: R) -> crate::Result<Range<'_>> {
        self.tree.create_range(range, Some(self.seqno))
    }

    /// Returns an iterator over a prefixed set of items in the snapshot.
    ///
    /// Avoid using an empty prefix as it may scan a lot of items (unless limited).
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn prefix<K: Into<Vec<u8>>>(&self, prefix: K) -> crate::Result<Prefix<'_>> {
        self.tree.create_prefix(prefix, Some(self.seqno))
    }

    /// Returns the first key-value pair in the snapshot.
    /// The key in this pair is the minimum key in the snapshot.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn first_key_value(&self) -> crate::Result<Option<(Vec<u8>, Vec<u8>)>> {
        self.tree
            .create_iter(Some(self.seqno))?
            .into_iter()
            .next()
            .transpose()
    }

    /// Returns the las key-value pair in the snapshot.
    /// The key in this pair is the maximum key in the snapshot.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn last_key_value(&self) -> crate::Result<Option<(Vec<u8>, Vec<u8>)>> {
        self.tree
            .create_iter(Some(self.seqno))?
            .into_iter()
            .next_back()
            .transpose()
    }

    /// Returns `true` if the snapshot contains the specified key.
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
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn is_empty(&self) -> crate::Result<bool> {
        self.first_key_value().map(|x| x.is_none())
    }

    /// Scans the entire snapshot, returning the amount of items.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    #[deprecated(
        note = "len() isn't deprecated per se, however it performs a full snapshot scan and should be avoided"
    )]
    pub fn len(&self) -> crate::Result<usize> {
        Ok(self.iter()?.into_iter().filter(Result::is_ok).count())
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
