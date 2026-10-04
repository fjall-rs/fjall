mod fifo;

use fifo::Queue;
use lsm_tree::{AbstractTree, SequenceNumberCounter, ValueType};
use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering::Acquire},
        Arc, RwLock,
    },
};

use crate::{
    journal::Journal, snapshot_tracker::SnapshotTracker, write_buffer_manager::WriteBufferManager,
};

type WriteItem = crate::batch::item::Item;

pub struct WriteRecord {
    state: AtomicU8,
    persist_mode: u8,
    // TODO: should be enum { item: WriteItem | batch: Vec<WriteItem> }
    batch: Vec<WriteItem>,
    assigned_seqno: AtomicU64,
}

impl WriteRecord {
    pub fn new(batch: Vec<WriteItem>) -> Self {
        WriteRecord {
            state: AtomicU8::default(),
            persist_mode: 0,
            batch,
            assigned_seqno: AtomicU64::default(),
        }
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

    pub fn commit(&self, record: Arc<WriteRecord>) {
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

                // Process records
                for _ in 0..1_000 {
                    // TODO: sum up batch sizes up until 1 MB or so...

                    let Some(write_record) = self.queue.try_pop() else {
                        break;
                    };

                    let batch_seqno = self.seqno.next();

                    journal_writer
                        .write_batch(
                            write_record.batch.iter(),
                            write_record.batch.len(),
                            batch_seqno,
                        )
                        // TODO: handle error
                        .unwrap();

                    let mut batch_size = 0;

                    // TODO: maybe we can use a stack alloc hashset/vec here, such as smallset
                    #[expect(clippy::mutable_key_type)]
                    let mut keyspaces_with_possible_stall = HashSet::new();

                    // TODO: std::mem::take batch instead... we don't own the batch because Arc... but really we do
                    for item in &write_record.batch {
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

                    write_record
                        .state
                        .store(1, std::sync::atomic::Ordering::Release);

                    write_record
                        .assigned_seqno
                        .store(batch_seqno, std::sync::atomic::Ordering::Release);

                    highest_seqno_published = Some(batch_seqno);

                    self.write_buffer_size.allocate(batch_size);

                    // TODO: how to do write stalling etc: like this?...
                    // Check each affected keyspace for write stall/halt
                    for keyspace in &keyspaces_with_possible_stall {
                        let memtable_size = keyspace.tree.active_memtable().size();
                        keyspace.check_memtable_rotate(memtable_size);
                        keyspace.local_backpressure();
                    }
                }

                // TODO: fsync or whatever persist wants to do
                // TODO: to do that, we will need the max. durability level of all the
                // items we have written previously
                // TODO: also, handle error
                journal_writer
                    .persist(crate::PersistMode::SyncData)
                    .unwrap();

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

                return;
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

                return;
            }
        }
    }
}
