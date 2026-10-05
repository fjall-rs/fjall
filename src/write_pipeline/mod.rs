mod fifo;

use crate::{
    journal::Journal, snapshot_tracker::SnapshotTracker, write_buffer_manager::WriteBufferManager,
    PersistMode,
};
use fifo::Queue;
use lsm_tree::{AbstractTree, SequenceNumberCounter, ValueType};
use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering::Acquire},
        Arc, RwLock,
    },
};

type BatchEntry = crate::batch::item::Item;

pub enum Batch {
    Single(BatchEntry),
    Batch(Vec<BatchEntry>),
}

pub struct WriteRecord {
    state: AtomicU8,
    persist_mode: Option<PersistMode>,
    batch: Batch,
    assigned_seqno: AtomicU64,
}

impl WriteRecord {
    pub fn new(batch: Batch) -> Self {
        WriteRecord {
            state: AtomicU8::default(),
            persist_mode: None,
            batch,
            assigned_seqno: AtomicU64::default(),
        }
    }

    pub fn persist_mode(mut self, mode: Option<PersistMode>) -> Self {
        self.persist_mode = mode;
        self
    }
}

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
    pub fn new(
        journal: Arc<Journal>,
        seqno: SequenceNumberCounter,
        snapshot_tracker: SnapshotTracker,
        write_buffer_size: WriteBufferManager,
    ) -> Self {
        Self {
            queue: Queue::with_capacity(1_024),
            master_lease: AtomicBool::new(true),
            lock: RwLock::default(),
            journal,
            seqno,
            snapshot_tracker,
            write_buffer_size,
        }
    }

    pub fn commit(&self, record: Arc<WriteRecord>) -> crate::Result<()> {
        let lock = self.lock.read().unwrap();

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
                let mut journal_writer = self.journal.get_writer().expect("lock is poisoned");

                // TODO: Check the poisoned flag after getting journal mutex, otherwise TOCTOU

                let mut highest_seqno_published = None;
                let mut collected_persist: Option<PersistMode> = None;

                // Process records
                for _ in 0..1_000 {
                    // TODO: sum up batch sizes up until 1 MB or so...

                    let Some(write_record) = self.queue.try_pop() else {
                        break;
                    };

                    let batch_seqno = self.seqno.next();

                    match &write_record.batch {
                        Batch::Single(item) => {
                            journal_writer.write_raw(
                                item.keyspace.id(),
                                &item.key,
                                &item.value,
                                item.value_type,
                                batch_seqno,
                            )?;
                        }
                        Batch::Batch(batch) => {
                            journal_writer.write_batch(batch.iter(), batch.len(), batch_seqno)?;
                        }
                    }

                    let mut batch_size = 0;

                    // TODO: maybe we can use a stack alloc hashset/vec here, such as smallset
                    #[expect(clippy::mutable_key_type)]
                    let mut keyspaces_with_possible_stall = HashSet::new();

                    match &write_record.batch {
                        Batch::Single(item) => {
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

                            {
                                let memtable_size = item.keyspace.tree.active_memtable().size();
                                item.keyspace.check_memtable_rotate(memtable_size);
                                item.keyspace.local_backpressure();
                            }
                        }
                        Batch::Batch(items) => {
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

                drop(journal_writer);

                {
                    let _lock = self.lock.write().unwrap();

                    if let Some(head) = self.queue.peek() {
                        head.state.store(2, std::sync::atomic::Ordering::Release);
                    } else {
                        self.master_lease
                            .store(true, std::sync::atomic::Ordering::Release);
                    }
                }

                return Ok(());
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

                return Ok(());
            }
        }
    }
}
