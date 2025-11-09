/// Guard to access key-value pairs
pub struct Guard<T: lsm_tree::Guard>(pub(crate) T);

impl<T: lsm_tree::Guard> Guard<T> {
    /// Accesses the key-value pair if the predicate returns `true`.
    ///
    /// The predicate receives the key - if returning `false`, the value
    /// may not be loaded if the tree is key-value separated.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn into_inner_if(
        self,
        pred: impl Fn(&[u8]) -> bool,
    ) -> crate::Result<Option<crate::KvPair>> {
        self.0.into_inner_if(pred).map_err(Into::into)
    }

    /// Returns the key-value tuple.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn into_inner(self) -> crate::Result<crate::KvPair> {
        self.0.into_inner().map_err(Into::into)
    }

    /// Returns the key.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn key(self) -> crate::Result<crate::UserKey> {
        self.0.key().map_err(Into::into)
    }

    /// Returns the value size.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn size(self) -> crate::Result<u32> {
        self.0.size().map_err(Into::into)
    }

    /// Returns the value.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn value(self) -> crate::Result<crate::UserValue> {
        self.0.value().map_err(Into::into)
    }
}
