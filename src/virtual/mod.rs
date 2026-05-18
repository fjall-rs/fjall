use crate::{Iter, Keyspace};
use byteview::ByteView;
use lsm_tree::UserValue;
use std::ops::RangeBounds;

/// A virtual keyspace that virtualizes a keyspace on top of an inner keyspace by using a prefix
///
/// This allows easily sharing one physical keyspace for multiple logical collections.
///
/// # Example
///
/// ```
/// # use fjall::{Database, KeyspaceCreateOptions, VirtualKeyspace};
/// #
/// # let folder = tempfile::tempdir()?;
/// # let db = Database::builder(folder).open()?;
/// let ks = db.keyspace("default", KeyspaceCreateOptions::default)?;
///
/// let users = VirtualKeyspace::new([0], ks.clone());
/// let relationships = VirtualKeyspace::new([1], ks.clone());
///
/// users.insert("user0", "Jack Doe")?;
/// users.insert("user1", "John Doe")?;
/// users.insert("user2", "Jane Doe")?;
///
/// relationships.insert("user0#user1", b"...")?;
/// relationships.insert("user1#user0", b"...")?;
///
/// assert_eq!(users.get("user0")?.as_deref(), Some(b"Jack Doe".as_slice()));
/// assert!(relationships.contains_key("user0#user1")?);
/// assert!(relationships.contains_key("user1#user0")?);
/// assert_eq!(3, users.len()?);
/// assert_eq!(2, relationships.len()?);
/// #
/// # Ok::<(), fjall::Error>(())
/// ```
pub struct VirtualKeyspace {
    // TODO: name?
    prefix: ByteView,
    inner: Keyspace,
}

impl VirtualKeyspace {
    /// Creates a new `VirtualKeyspace` with the given prefix and inner keyspace.
    pub fn new(prefix: impl Into<ByteView>, inner: Keyspace) -> Self {
        Self {
            prefix: prefix.into(),
            inner,
        }
    }

    /// Returns a reference to the inner keyspace.
    pub fn inner(&self) -> &Keyspace {
        &self.inner
    }

    /// Scans the entire virtual keyspace, returning the number of items.
    ///
    /// # Caution
    ///
    /// This operation scans the entire virtual keyspace: O(n) complexity!
    ///
    /// Never, under any circumstances, use .`len()` == 0 to check
    /// if the keyspace is empty, use [`VirtualKeyspace::is_empty`] instead.
    pub fn len(&self) -> crate::Result<usize> {
        let mut sum = 0;
        for g in self.inner.prefix(&self.prefix) {
            let _ = g.key()?;
            sum += 1;
        }
        Ok(sum)
    }

    /// Gets the value associated with the given key in the virtual keyspace.
    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> crate::Result<Option<UserValue>> {
        self.inner.get(ByteView::fused(&self.prefix, key.as_ref()))
    }

    /// Returns `true` if the virtual keyspace contains no KVs.
    ///
    /// This operation has O(log N) complexity.
    pub fn is_empty(&self) -> crate::Result<bool> {
        Ok(self.iter().next().map(|x| x.key()).transpose()?.is_none())
    }

    /// Returns `true` if the virtual keyspace contains the given key.
    pub fn contains_key<K: AsRef<[u8]>>(&self, key: K) -> crate::Result<bool> {
        self.inner
            .contains_key(ByteView::fused(&self.prefix, key.as_ref()))
    }

    /// Returns an iterator over the KVs in the virtual keyspace.
    pub fn iter(&self) -> Iter {
        self.inner.prefix(&self.prefix)
    }

    /// Returns an iterator over the KVs in the virtual keyspace that are in the given range.
    pub fn range<K: AsRef<[u8]>, R: RangeBounds<K>>(&self, range: R) -> Iter {
        self.inner
            .range(crate::util::prefixed_range(&self.prefix, range))
    }

    /// Returns an iterator over the KVs in the virtual keyspace that have the given prefix.
    pub fn prefix<K: AsRef<[u8]>>(&self, prefix: K) -> Iter {
        self.inner
            .prefix(&ByteView::fused(&self.prefix, prefix.as_ref()))
    }

    /// Inserts a key-value pair into the virtual keyspace.
    pub fn insert<K: AsRef<[u8]>, V: Into<UserValue>>(
        &self,
        key: K,
        value: V,
    ) -> crate::Result<()> {
        self.inner
            .insert(ByteView::fused(&self.prefix, &key.as_ref()), value)
    }

    /// Removes a key from the virtual keyspace.
    pub fn remove<K: AsRef<[u8]>>(&self, key: K) -> crate::Result<()> {
        self.inner
            .remove(ByteView::fused(&self.prefix, &key.as_ref()))
    }

    /// Removes an item from the keyspace, leaving behind a weak tombstone.
    ///
    /// When a weak tombstone is matched with a single write in a compaction,
    /// the tombstone will be removed along with the value. If the key was
    /// overwritten the result of a `remove_weak` is undefined.
    ///
    /// Only use this remove if it is known that the key has only been written
    /// to once since its creation or last `remove_weak`.
    ///
    /// The key may be up to 65536 bytes long.
    /// Shorter keys result in better performance.
    ///
    /// # Experimental
    ///
    /// This function is currently experimental.
    #[doc(hidden)]
    pub fn remove_weak<K: AsRef<[u8]>>(&self, key: K) -> crate::Result<()> {
        self.inner
            .remove_weak(ByteView::fused(&self.prefix, &key.as_ref()))
    }
}
