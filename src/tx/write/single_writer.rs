use std::{
    ops::{Deref, DerefMut},
    sync::MutexGuard,
};

use crate::{snapshot_nonce::SnapshotNonce, PersistMode, TxKeyspace};

use super::WriteTransaction as InnerWriteTransaction;

pub struct WriteTransaction<'a> {
    _guard: MutexGuard<'a, ()>,
    inner: InnerWriteTransaction,
}

impl<'a> WriteTransaction<'a> {
    pub(crate) fn new(
        keyspace: TxKeyspace,
        nonce: SnapshotNonce,
        guard: MutexGuard<'a, ()>,
    ) -> Self {
        Self {
            _guard: guard,
            inner: InnerWriteTransaction::new(keyspace, nonce),
        }
    }

    /// Sets the durability level.
    #[must_use]
    pub fn durability(mut self, mode: Option<PersistMode>) -> Self {
        self.inner = self.inner.durability(mode);
        self
    }

    pub fn commit(self) -> crate::Result<()> {
        self.inner.commit()
    }

    pub fn rollback(self) {
        self.inner.rollback();
    }
}

impl<'a> Deref for WriteTransaction<'a> {
    type Target = super::WriteTransaction;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<'a> DerefMut for WriteTransaction<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}
