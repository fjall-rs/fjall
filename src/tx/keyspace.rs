use super::{partition::TxPartitionHandle, Transaction};
use crate::{Config, Keyspace, PartitionCreateOptions};
use std::sync::{Arc, Mutex};

/// Transaction keyspace
#[allow(clippy::module_name_repetitions)]
pub struct TransactionalKeyspace {
    inner: Keyspace,
    tx_lock: Arc<Mutex<()>>,
}

/// Alias for [`TransactionalKeyspace`]
#[allow(clippy::module_name_repetitions)]
pub type TxKeyspace = TransactionalKeyspace;

impl TxKeyspace {
    /// Starts a new writeable transaction.
    #[must_use]
    pub fn write_tx(&self) -> Transaction {
        let instant = self.inner.instant();
        Transaction::new(self.tx_lock.lock().expect("lock is poisoned"), instant)
    }

    /// Creates or opens a keyspace partition.
    ///
    /// # Errors
    ///
    /// Returns error, if an IO error occured.
    ///
    /// # Panics
    ///
    /// Panics if the partition name includes characters other than: a-z A-Z 0-9 _ -
    pub fn open_partition(
        &self,
        name: &str,
        create_options: PartitionCreateOptions,
    ) -> crate::Result<TxPartitionHandle> {
        let partition = self.inner.open_partition(name, create_options)?;

        Ok(TxPartitionHandle {
            inner: partition,
            tx_lock: self.tx_lock.clone(),
        })
    }

    /// Opens a keyspace in the given directory.
    ///
    /// # Errors
    ///
    /// Returns error, if an IO error occured.
    pub fn open(config: Config) -> crate::Result<Self> {
        let inner = Keyspace::create_or_recover(config)?;
        inner.start_background_threads();

        Ok(Self {
            inner,
            tx_lock: Arc::default(),
        })
    }
}
