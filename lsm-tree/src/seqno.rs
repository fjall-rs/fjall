use crate::SeqNo;
use std::sync::{
    atomic::{
        AtomicU64,
        Ordering::{Acquire, Release},
    },
    Arc,
};

/// Thread-safe sequence number generator
///
/// # Examples
///
/// ```
/// # use lsm_tree::{Config, SequenceNumberCounter};
/// #
/// # let path = tempfile::tempdir()?;
/// let tree = Config::new(path).open()?;
///
/// let seqno = SequenceNumberCounter::default();
///
/// // Do some inserts...
/// tree.insert("a".as_bytes(), "abc", seqno.next());
/// tree.insert("b".as_bytes(), "abc", seqno.next());
/// tree.insert("c".as_bytes(), "abc", seqno.next());
///
/// // Maybe create a snapshot
/// let snapshot = tree.snapshot(seqno.get());
///
/// // Create a batch
/// let batch_seqno = seqno.next();
/// tree.remove("a".as_bytes(), batch_seqno);
/// tree.remove("b".as_bytes(), batch_seqno);
/// tree.remove("c".as_bytes(), batch_seqno);
/// #
/// # assert!(tree.is_empty()?);
/// # Ok::<(), lsm_tree::Error>(())
/// ```
#[derive(Clone, Default, Debug)]
pub struct SequenceNumberCounter(Arc<AtomicU64>);

impl std::ops::Deref for SequenceNumberCounter {
    type Target = Arc<AtomicU64>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl SequenceNumberCounter {
    /// Creates a new counter, setting it to some previous value
    #[must_use]
    pub fn new(prev: SeqNo) -> Self {
        Self(Arc::new(AtomicU64::new(prev)))
    }

    /// Gets the next sequence number.
    ///
    /// This should only be used when creating a snapshot.
    #[must_use]
    pub fn get(&self) -> SeqNo {
        self.load(Acquire)
    }

    /// Gets the next sequence number.
    #[must_use]
    pub fn next(&self) -> SeqNo {
        self.fetch_add(1, Release)
    }
}
