// Copyright (c) 2026-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

mod fifo;
mod record;

use crate::{
    journal::Journal, poison::PoisonSignal, snapshot_tracker::SnapshotTracker,
    write_buffer_manager::WriteBufferManager, write_pipeline::record::WRITE_RECORD_STATE_PENDING,
    Conflict, PersistMode,
};
use fifo::Queue;
use lsm_tree::{AbstractTree, SequenceNumberCounter, ValueType};
use record::{
    WRITE_RECORD_STATE_CONFLICTED, WRITE_RECORD_STATE_LEADER_ABDICATED,
    WRITE_RECORD_STATE_PUBLISHED,
};
use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicBool, Ordering::Acquire},
        Arc,
    },
};

pub use record::{OptimisticConflictCheck, Payload as WriteRecordPayload, WriteRecord};

const WRITE_PIPELINE_QUEUE_CAPACITY: usize = 2_usize.pow(10);

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
    leader_lease: AtomicBool,
    poison_flag: PoisonSignal,
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
        poison_flag: PoisonSignal,
    ) -> Self {
        Self {
            queue: Queue::with_capacity(WRITE_PIPELINE_QUEUE_CAPACITY),
            leader_lease: AtomicBool::new(true),
            journal,
            seqno,
            snapshot_tracker,
            write_buffer_size,
            poison_flag,
        }
    }

    fn act_as_leader(&self, record: Arc<WriteRecord>) -> crate::Result<Result<(), Conflict>> {
        log::trace!("getting journal writer");

        let mut journal_writer = self
            .journal
            .get_writer()
            .map_err(|_| crate::Error::Poisoned)?;

        log::trace!("got journal writer");

        // IMPORTANT: Check the poisoned flag after getting journal mutex, otherwise TOCTOU
        if self.poison_flag.is_poisoned() {
            return Err(crate::Error::Poisoned);
        }

        let mut highest_seqno_published = None;
        let mut collected_persist: Option<PersistMode> = None;
        let mut bytes_written = 0;

        // Process records
        for _ in 0..100_000 {
            let Some(write_record) = self.queue.try_pop() else {
                break;
            };

            if let Some(occ) = &write_record.occ {
                if occ.oracle.has_conflict(
                    occ.instant,
                    occ.conflict_manager
                        .lock()
                        .expect("conflict manager should not be poisoned")
                        .as_ref()
                        .expect("conflict manager should exist"),
                )? {
                    // Mark as conflicted
                    write_record.set_conflicted();
                    continue;
                }
            }

            let batch_seqno = self.seqno.next();

            #[expect(
                clippy::expect_used,
                reason = "write record can only be initialized with payload, and only the leader consumes write records (exactly once)"
            )]
            let payload = write_record
                .payload
                .take()
                .expect("should get write record payload");

            bytes_written += match &payload {
                WriteRecordPayload::Single(item) => journal_writer.write_raw(
                    item.keyspace.id(),
                    &item.key,
                    &item.value,
                    item.value_type,
                    batch_seqno,
                ),
                WriteRecordPayload::Multiple(batch) => {
                    journal_writer.write_batch(batch.iter(), batch_seqno)
                }
            }
            .inspect_err(|e| {
                log::error!("journal write failed, which is a FATAL, and possibly hardware-related, failure: {e:?}");
            })?;

            let mut batch_size = 0;

            // TODO: maybe we can use a stack alloc hashset/vec here, such as smallset
            #[expect(clippy::mutable_key_type)]
            let mut keyspaces_with_possible_stall = HashSet::new();

            match payload {
                WriteRecordPayload::Single(item) => {
                    // TODO: need a better, generic write op in lsm-tree?
                    let (item_size, _) = match item.value_type {
                        ValueType::Value => {
                            item.keyspace.tree.insert(item.key, item.value, batch_seqno)
                        }
                        ValueType::Tombstone => item.keyspace.tree.remove(item.key, batch_seqno),
                        ValueType::WeakTombstone => {
                            item.keyspace.tree.remove_weak(item.key, batch_seqno)
                        }
                        ValueType::Indirection => unreachable!(),
                    };

                    batch_size += item_size;
                }
                WriteRecordPayload::Multiple(items) => {
                    for item in items {
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

            write_record.set_published();

            write_record
                .assigned_seqno
                .store(batch_seqno, std::sync::atomic::Ordering::Release);

            highest_seqno_published = Some(batch_seqno);

            if let Some(occ) = &write_record.occ {
                occ.oracle.finalize(
                    batch_seqno + 1,
                    occ.conflict_manager
                        .lock()
                        .expect("conflict manager should not be poisoned")
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
            // TODO: maybe we can create a "global" function and pass in the keyspaces instead

            // Check each affected keyspace for write stall/halt
            for keyspace in &keyspaces_with_possible_stall {
                let memtable_size = keyspace.tree.active_memtable().size();
                keyspace.check_memtable_rotate(memtable_size);
                keyspace.local_backpressure();
            }

            // Limit the bytes written per leader term to
            // avoid tail latencies growing too large for any
            // given writer
            if bytes_written >= 1_000_000 {
                break;
            }
        }

        if let Some(persist_mode) = collected_persist {
            journal_writer
                .persist(persist_mode)
                .inspect_err(|e| {
                    log::error!("persist failed, which is a FATAL, and possibly hardware-related, failure: {e:?}");
                })?;
        }

        if let Some(seqno) = highest_seqno_published {
            self.snapshot_tracker.publish(seqno);
        }

        drop(journal_writer);

        self.abdicate();

        if record.state() == WRITE_RECORD_STATE_CONFLICTED {
            return Ok(Err(Conflict));
        }

        Ok(Ok(()))
    }

    /// Promotes the queue head, or frees the leader lease if the queue is empty.
    //
    // IMPORTANT: Only call this as leader!!!
    fn abdicate(&self) {
        if let Some(head) = self.queue.peek() {
            // NOTE: If the queue has entry, there must be a follower
            // The follower will see the abdication status and
            // become leader, so no need to free the leader lease
            head.set_abdicated();
        } else {
            self.leader_lease
                .store(true, std::sync::atomic::Ordering::Release);
        }
    }

    fn try_acquire_leader_lease(&self) -> bool {
        self.leader_lease
            .compare_exchange(
                true,
                false,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Relaxed,
            )
            .is_ok()
    }

    /// Tries to push a record until it is definitely enqueued.
    fn push_write_record(&self, record: &Arc<WriteRecord>) {
        while self.queue.try_push(record.clone()).is_none() {
            std::hint::spin_loop();
        }
    }

    /// Appends the write record to the write pipeline.
    ///
    /// When returning, the write record is guaranteed to be persisted matching
    /// the record's durability parameter (or stronger).
    pub fn commit(&self, record: Arc<WriteRecord>) -> crate::Result<Result<(), Conflict>> {
        self.push_write_record(&record);

        let mut is_leader = self.try_acquire_leader_lease();

        'start: loop {
            if is_leader {
                return self.act_as_leader(record).inspect_err(|_| {
                    self.poison_flag.poison();
                    self.abdicate();
                });
            }

            // Spin on record state until it is processed
            for i in 0.. {
                match record.state() {
                    WRITE_RECORD_STATE_PENDING => {
                        std::hint::spin_loop();

                        // We need to do this to catch leader abdications
                        //
                        // Imagine this TOCTOU scenario:
                        //   Existing leader sees no queue head
                        //     -> We now push a write record & become follower
                        //       -> Leader frees leader lease
                        //         -> We spin on record state forever
                        if i % 1_000 == 0 {
                            if self.try_acquire_leader_lease() {
                                // Crown ourselves as leader
                                is_leader = true;
                                continue 'start;
                            }
                        }
                    }
                    WRITE_RECORD_STATE_PUBLISHED => break,
                    WRITE_RECORD_STATE_LEADER_ABDICATED => {
                        is_leader = true;
                        continue 'start;
                    }
                    WRITE_RECORD_STATE_CONFLICTED => return Ok(Err(Conflict)),
                    _ => unreachable!(),
                }
            }

            // NOTE: Even if we had persist_mode=None, we still need to wait for the
            // record contents to be visible, otherwise our writes may not be visible
            // by a reader immediately following this commit

            let assigned_seqno = record.assigned_seqno.load(Acquire);

            // Spin on the visible seqno until our record is visible
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
