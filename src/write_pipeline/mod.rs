mod fifo;

use crate::{
    journal::Journal, snapshot_tracker::SnapshotTracker, tx::optimistic::Oracle,
    write_buffer_manager::WriteBufferManager, Conflict, PersistMode,
};
use fifo::Queue;
use lsm_tree::{AbstractTree, SeqNo, SequenceNumberCounter, ValueType};
use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering::Acquire},
        Arc, Mutex, RwLock,
    },
};

type BatchEntry = crate::batch::item::Item;

pub enum WriteRecordData {
    /// A single item to avoid the Vec's heap allocation
    /// for single item writes (e.g. `Keyspace::insert`)
    Single(BatchEntry),

    /// An atomic write batch
    Multiple(Vec<BatchEntry>),
}

pub struct OptimisticConflictCheck {
    oracle: Arc<Oracle>,
    instant: SeqNo,
    conflict_manager: Mutex<Option<crate::tx::optimistic::ConflictManager>>,
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

pub struct WriteRecord {
    // TODO: can we use an atomic u8 enum?
    /// Record state (0 = unfinished, 1 = published, 2 = published & master stepped down, 3 = conflicted (OCC))
    state: AtomicU8,

    /// This write records's durability requirement
    persist_mode: Option<PersistMode>,

    /// The actual user data to write
    data: WriteRecordData,

    /// The record's seqno
    ///
    /// This is initially 0, and is assigned by the master while processing
    /// the write records.
    ///
    /// As long as state is 0, this value is invalid.
    assigned_seqno: AtomicU64,

    // TODO: need to pass instant and CM as well
    occ: Option<OptimisticConflictCheck>,
}

impl WriteRecord {
    /// Creates a new write record.
    pub fn new(data: WriteRecordData) -> Self {
        WriteRecord {
            state: AtomicU8::default(),
            persist_mode: None,
            data,
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

/// A write pipeline using flat combining to allow grouped commits
/// and better concurrent throughput
///
/// See the paper on flat combining: https://dl.acm.org/doi/10.1145/1810479.1810540
pub struct WritePipeline {
    write_buffer_size: WriteBufferManager,
    seqno: SequenceNumberCounter,
    snapshot_tracker: SnapshotTracker,
    journal: Arc<Journal>,
    queue: Queue<Arc<WriteRecord>>,
    master_lease: AtomicBool,
    lock: RwLock<()>,
}

impl WritePipeline {
    /// Creates a new write pipeline.
    ///
    /// One database has one write pipeline.
    pub fn new(
        journal: Arc<Journal>,
        seqno: SequenceNumberCounter,
        snapshot_tracker: SnapshotTracker,
        write_buffer_size: WriteBufferManager,
    ) -> Self {
        Self {
            queue: Queue::with_capacity(1_024), // todo: is 1024 enough?
            master_lease: AtomicBool::new(true),
            lock: RwLock::default(),
            journal,
            seqno,
            snapshot_tracker,
            write_buffer_size,
        }
    }

    // TODO: maybe use CommitOutcome instead of double-Result

    /// Appends the write record to the write pipeline.
    ///
    /// When returning, the write record is guaranteed to be persisted matching
    /// the record's durability parameter (or stronger).
    pub fn commit(&self, record: Arc<WriteRecord>) -> crate::Result<Result<(), Conflict>> {
        let lock = self.lock.read().map_err(|_| crate::Error::Poisoned)?;

        while self.queue.try_push(record.clone()).is_none() {}

        let mut is_master = self
            .master_lease
            .compare_exchange(
                true,
                false,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Relaxed,
            )
            .is_ok();

        drop(lock);

        'start: loop {
            if is_master {
                log::trace!("getting journal writer");

                let mut journal_writer = self
                    .journal
                    .get_writer()
                    .map_err(|_| crate::Error::Poisoned)?;

                log::trace!("got journal writer");

                // TODO: Check the poisoned flag after getting journal mutex, otherwise TOCTOU

                let mut highest_seqno_published = None;
                let mut collected_persist: Option<PersistMode> = None;

                // Process records
                for _ in 0..1_000 {
                    // TODO: sum up batch sizes up until 1 MB or so...

                    let Some(write_record) = self.queue.try_pop() else {
                        break;
                    };

                    // TODO: clean up OCC stuff to only take locks once if possible

                    if let Some(occ) = &write_record.occ {
                        log::warn!("OCC conflict");

                        if occ.oracle.has_conflict(
                            occ.instant,
                            occ.conflict_manager
                                .lock()
                                .map_err(|_| crate::Error::Poisoned)?
                                .as_ref()
                                .expect("conflict manager should exist"),
                        )? {
                            // Mark as conflicted
                            write_record
                                .state
                                .store(3, std::sync::atomic::Ordering::Release);

                            continue;
                        }
                    }

                    log::info!("OCC conflict done");

                    let batch_seqno = self.seqno.next();

                    match &write_record.data {
                        WriteRecordData::Single(item) => {
                            journal_writer.write_raw(
                                item.keyspace.id(),
                                &item.key,
                                &item.value,
                                item.value_type,
                                batch_seqno,
                            )?;
                        }
                        WriteRecordData::Multiple(batch) => {
                            journal_writer.write_batch(batch.iter(), batch.len(), batch_seqno)?;
                        }
                    }

                    let mut batch_size = 0;

                    // TODO: maybe we can use a stack alloc hashset/vec here, such as smallset
                    let mut keyspaces_with_possible_stall = HashSet::new();

                    match &write_record.data {
                        WriteRecordData::Single(item) => {
                            // TODO: std::mem::take item instead...?
                            let item = item.clone();

                            // TODO: need a better, generic write op
                            let (item_size, _) = match item.value_type {
                                ValueType::Value => {
                                    item.keyspace.tree.insert(item.key, item.value, batch_seqno)
                                }
                                ValueType::Tombstone => {
                                    item.keyspace.tree.remove(item.key, batch_seqno)
                                }
                                ValueType::WeakTombstone => {
                                    item.keyspace.tree.remove_weak(item.key, batch_seqno)
                                }
                                ValueType::Indirection => unreachable!(),
                            };

                            batch_size += item_size;

                            let memtable_size = item.keyspace.tree.active_memtable().size();
                            item.keyspace.check_memtable_rotate(memtable_size);
                            item.keyspace.local_backpressure();
                        }
                        WriteRecordData::Multiple(items) => {
                            // TODO: std::mem::take batch instead... we don't own the batch because Arc... but really we do?
                            for item in items {
                                let item = item.clone();

                                // TODO: need a better, generic write op
                                let (item_size, _) = match item.value_type {
                                    ValueType::Value => {
                                        item.keyspace.tree.insert(item.key, item.value, batch_seqno)
                                    }
                                    ValueType::Tombstone => {
                                        item.keyspace.tree.remove(item.key, batch_seqno)
                                    }
                                    ValueType::WeakTombstone => {
                                        item.keyspace.tree.remove_weak(item.key, batch_seqno)
                                    }
                                    ValueType::Indirection => unreachable!(),
                                };

                                batch_size += item_size;

                                // IMPORTANT: Clone the handle, because we don't want to keep the keyspaces lock open
                                keyspaces_with_possible_stall.insert(item.keyspace.clone());
                            }
                        }
                    }

                    write_record
                        .state
                        .store(1, std::sync::atomic::Ordering::Release);

                    write_record
                        .assigned_seqno
                        .store(batch_seqno, std::sync::atomic::Ordering::Release);

                    highest_seqno_published = Some(batch_seqno);

                    if let Some(occ) = &write_record.occ {
                        log::info!("OCC finalize");

                        occ.oracle.finalize(
                            batch_seqno + 1,
                            occ.conflict_manager
                                .lock()
                                .map_err(|_| crate::Error::Poisoned)?
                                .take()
                                .expect("conflict manager should exist"),
                        )?;
                    }

                    collected_persist = match (collected_persist, write_record.persist_mode) {
                        (Some(prev), Some(curr)) => Some(prev.max(curr)),
                        (None, Some(curr)) => Some(curr),
                        (Some(prev), _) => Some(prev),
                        _ => collected_persist,
                    };

                    self.write_buffer_size.allocate(batch_size);

                    // TODO: how to do write stalling etc: like this?...
                    // Check each affected keyspace for write stall/halt
                    for keyspace in &keyspaces_with_possible_stall {
                        let memtable_size = keyspace.tree.active_memtable().size();
                        keyspace.check_memtable_rotate(memtable_size);
                        keyspace.local_backpressure();
                    }
                }

                if let Some(persist_mode) = collected_persist {
                    journal_writer.persist(persist_mode)?;
                }

                if let Some(seqno) = highest_seqno_published {
                    self.snapshot_tracker.publish(seqno);
                }

                // TODO: do remaining OCC GC work here...

                drop(journal_writer);

                {
                    let _lock = self.lock.write().map_err(|_| crate::Error::Poisoned)?;

                    if let Some(head) = self.queue.peek() {
                        head.state.store(2, std::sync::atomic::Ordering::Release);
                    } else {
                        self.master_lease
                            .store(true, std::sync::atomic::Ordering::Release);
                    }
                }

                if record.state.load(std::sync::atomic::Ordering::Acquire) == 3 {
                    return Ok(Err(Conflict));
                }

                return Ok(Ok(()));
            } else {
                // Spin on record state and then on visible seqno

                loop {
                    let state = record.state.load(std::sync::atomic::Ordering::Relaxed);

                    match state {
                        1 => break,
                        2 => {
                            is_master = true;
                            continue 'start;
                        }
                        3 => return Ok(Err(Conflict)),
                        _ => {}
                    }
                }

                let assigned_seqno = record.assigned_seqno.load(Acquire);

                loop {
                    let visible_seqno = self.snapshot_tracker.get();

                    if visible_seqno > assigned_seqno {
                        break;
                    }
                }

                return Ok(Ok(()));
            }
        }
    }
}
