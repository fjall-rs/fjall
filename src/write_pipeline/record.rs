use lsm_tree::SeqNo;
use take_once::TakeOnce;

use crate::{tx::optimistic::Oracle, PersistMode};
use std::sync::{
    atomic::{AtomicU64, AtomicU8},
    Arc, Mutex,
};

/// Write record is pending
pub const WRITE_RECORD_STATE_PENDING: u8 = 0;

/// Write record is published
pub const WRITE_RECORD_STATE_PUBLISHED: u8 = 1;

/// Write record is published & leader stepped down
pub const WRITE_RECORD_STATE_LEADER_ABDICATED: u8 = 2;

/// Write record is conflicted by OCC
pub const WRITE_RECORD_STATE_CONFLICTED: u8 = 3;

pub struct OptimisticConflictCheck {
    pub(super) oracle: Arc<Oracle>,
    pub(super) instant: SeqNo,
    pub(super) conflict_manager: Mutex<Option<crate::tx::optimistic::ConflictManager>>,
}

impl OptimisticConflictCheck {
    pub fn new(
        oracle: Arc<Oracle>,
        instant: SeqNo,
        conflict_manager: crate::tx::optimistic::ConflictManager,
    ) -> Self {
        Self {
            oracle,
            instant,
            conflict_manager: Mutex::new(Some(conflict_manager)),
        }
    }
}

type BatchEntry = crate::batch::item::Item;

pub enum Payload {
    /// A single item to avoid the Vec's heap allocation
    /// for single item writes (e.g. `Keyspace::insert`)
    Single(BatchEntry),

    /// An atomic write batch
    Multiple(Vec<BatchEntry>),
}

pub struct WriteRecord {
    state: AtomicU8,

    /// This write record's durability requirement
    pub(super) persist_mode: Option<PersistMode>,

    /// The actual user data to write
    pub(super) payload: TakeOnce<Payload>,

    /// The record's seqno
    ///
    /// This is initially 0, and is assigned by the master while processing
    /// the write records.
    ///
    /// As long as state is 0, this value is invalid.
    pub(super) assigned_seqno: AtomicU64,

    pub(super) occ: Option<OptimisticConflictCheck>,
}

impl WriteRecord {
    pub fn state(&self) -> u8 {
        self.state.load(std::sync::atomic::Ordering::Acquire)
    }

    fn set_state(&self, state: u8) {
        self.state
            .store(state, std::sync::atomic::Ordering::Release);
    }

    pub fn set_published(&self) {
        self.set_state(WRITE_RECORD_STATE_PUBLISHED);
    }

    pub fn set_abdicated(&self) {
        self.set_state(WRITE_RECORD_STATE_LEADER_ABDICATED);
    }

    pub fn set_conflicted(&self) {
        self.set_state(WRITE_RECORD_STATE_CONFLICTED);
    }

    /// Creates a new write record.
    pub fn new(payload: Payload) -> Self {
        WriteRecord {
            state: AtomicU8::new(WRITE_RECORD_STATE_PENDING),
            persist_mode: None,
            payload: TakeOnce::new_with(payload),
            assigned_seqno: AtomicU64::default(),
            occ: None,
        }
    }

    /// Sets the write record's persist mode.
    ///
    /// All records combined by the master will use the highest durability
    /// level that was encountered.
    /// If buffer/fsync/fsyncdata, only a single syscall will be issued for all
    /// batches.
    pub fn persist_mode(mut self, mode: Option<PersistMode>) -> Self {
        self.persist_mode = mode;
        self
    }

    /// Enables optimistic conflict checking for this write record.
    pub fn with_occ(mut self, occ: OptimisticConflictCheck) -> Self {
        self.occ = Some(occ);
        self
    }
}
