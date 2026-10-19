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
use record::{WRITE_RECORD_STATE_CONFLICTED, WRITE_RECORD_STATE_PUBLISHED};
use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicBool, Ordering::Acquire},
        Arc,
    },
};

pub use record::{OptimisticConflictCheck, Payload as WriteRecordPayload, WriteRecord};

const WRITE_PIPELINE_QUEUE_CAPACITY: usize = 2_usize.pow(10);

pub struct State {
    pub journal: Arc<Journal>,
    pub write_buffer_size: WriteBufferManager,
    pub seqno: SequenceNumberCounter,
    pub snapshot_tracker: SnapshotTracker,
    pub poison_flag: PoisonSignal,
}

/// A write pipeline using flat combining to allow grouped commits
/// and better concurrent throughput
///
/// See the paper on flat combining: <https://dl.acm.org/doi/10.1145/1810479.1810540>
pub struct WritePipeline {
    queue: Queue<Arc<WriteRecord>>,
    leader_lease: AtomicBool,
    state: State,
    leader_capacity: usize,
}

impl WritePipeline {
    /// Creates a new write pipeline.
    ///
    /// One database has one write pipeline.
    pub fn with_capacity(state: State, leader_capacity: usize) -> Self {
        Self {
            queue: Queue::with_capacity(WRITE_PIPELINE_QUEUE_CAPACITY),
            leader_lease: AtomicBool::new(true),
            state,
            leader_capacity,
        }
    }

    fn act_as_leader(&self, record: Arc<WriteRecord>) -> crate::Result<Result<(), Conflict>> {
        let mut journal_writer = self
            .state
            .journal
            .get_writer()
            .map_err(|_| crate::Error::Poisoned)?;

        // IMPORTANT: Check the poisoned flag after getting journal mutex, otherwise TOCTOU
        if self.state.poison_flag.is_poisoned() {
            return Err(crate::Error::Poisoned);
        }

        let mut highest_seqno_published = None;
        let mut collected_persist: Option<PersistMode> = None;
        let mut bytes_written = 0;

        // TODO: maybe we can use a stack alloc hashset/vec here, such as smallset
        #[expect(clippy::mutable_key_type)]
        let mut keyspaces_with_possible_stall = HashSet::new();

        // Process records
        for _ in 0.. {
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

            let batch_seqno = self.state.seqno.next();

            #[expect(
                clippy::expect_used,
                reason = "write record can only be initialized with payload, and only the leader consumes write records (exactly once)"
            )]
            let payload = write_record
                .payload
                .take()
                .expect("should get write record payload");

            #[cfg(test)]
            if write_record.error_flag {
                return Err(crate::Error::Io(std::io::Error::other("injected failure")));
            }

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

                    keyspaces_with_possible_stall.insert(item.keyspace.clone());
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

            write_record
                .assigned_seqno
                .store(batch_seqno, std::sync::atomic::Ordering::Release);

            write_record.set_published();

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

            self.state.write_buffer_size.allocate(batch_size);

            // TODO: how to do write stalling etc: like this?...
            // TODO: maybe we can create a "global" function and pass in the keyspaces instead

            // Limit the bytes written per leader term to
            // avoid tail latencies growing too large for any
            // given writer
            //
            // Also, we cannot exit the loop until our record is processed.
            if bytes_written >= self.leader_capacity && !record.is_pending() {
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
            self.state.snapshot_tracker.publish(seqno);
        }

        drop(journal_writer);

        // Check each affected keyspace for write stall/halt
        for keyspace in &keyspaces_with_possible_stall {
            let memtable_size = keyspace.tree.active_memtable().size();
            keyspace.check_memtable_rotate(memtable_size);
            keyspace.local_backpressure();
        }

        self.abdicate();

        if record.state() == WRITE_RECORD_STATE_CONFLICTED {
            return Ok(Err(Conflict));
        }

        Ok(Ok(()))
    }

    /// Frees the leader lease.
    //
    // IMPORTANT: Only call this as leader!!!
    fn abdicate(&self) {
        self.leader_lease
            .store(true, std::sync::atomic::Ordering::Release);
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
    #[expect(clippy::collapsible_if)]
    fn push_write_record(&self, record: &Arc<WriteRecord>) -> crate::Result<()> {
        for i in 0.. {
            if self.queue.try_push(record.clone()).is_some() {
                return Ok(());
            }

            if i % 1_000 == 0 {
                if self.state.poison_flag.is_poisoned() {
                    return Err(crate::Error::Poisoned);
                }
            }

            std::hint::spin_loop();
        }

        Ok(())
    }

    #[cfg(test)]
    fn become_follower(&self, record: Arc<WriteRecord>) -> crate::Result<Result<(), Conflict>> {
        self.commit_inner(record, false)
    }

    #[expect(clippy::collapsible_if)]
    fn commit_inner(
        &self,
        record: Arc<WriteRecord>,
        mut is_leader: bool,
    ) -> crate::Result<Result<(), Conflict>> {
        'start: loop {
            if is_leader {
                return self.act_as_leader(record).inspect_err(|_| {
                    self.state.poison_flag.poison();
                    self.abdicate();
                });
            }

            // Spin on record state until it is processed
            for i in 0.. {
                match record.state() {
                    WRITE_RECORD_STATE_PENDING => {
                        // NOTE: We need to do this to catch leader abdications
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

                        std::hint::spin_loop();
                    }
                    WRITE_RECORD_STATE_PUBLISHED => break,
                    WRITE_RECORD_STATE_CONFLICTED => return Ok(Err(Conflict)),
                    _ => unreachable!(),
                }

                std::hint::spin_loop();
            }

            // NOTE: Even if we had persist_mode=None, we still need to wait for the
            // record contents to be visible, otherwise our writes may not be visible
            // by a reader immediately following this commit

            let assigned_seqno = record.assigned_seqno.load(Acquire);

            // Spin on the visible seqno until our record is visible
            for i in 0.. {
                let visible_seqno = self.state.snapshot_tracker.get();

                if visible_seqno > assigned_seqno {
                    break;
                }

                // See above why we do this
                if i % 1_000 == 0 {
                    if self.try_acquire_leader_lease() {
                        // Crown ourselves as leader
                        is_leader = true;
                        continue 'start;
                    }
                }

                std::hint::spin_loop();
            }

            return Ok(Ok(()));
        }
    }

    /// Appends the write record to the write pipeline.
    ///
    /// When returning, the write record is guaranteed to be persisted matching
    /// the record's durability parameter (or stronger).
    pub fn commit(&self, record: Arc<WriteRecord>) -> crate::Result<Result<(), Conflict>> {
        self.push_write_record(&record)?;
        self.commit_inner(record, self.try_acquire_leader_lease())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_log::test;

    #[test]
    fn write_pipeline_failure_simple() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;
        let db = crate::Database::builder(dir.path()).open()?;
        let keyspace = db.keyspace("items", Default::default)?;
        let write_pipeline = &db.supervisor.write_pipeline;

        write_pipeline.push_write_record(&Arc::new(
            WriteRecord::new(WriteRecordPayload::Single(crate::batch::item::Item::new(
                keyspace.clone(),
                b"a".into(),
                b"a".into(),
                ValueType::Value,
            )))
            // Inject I/O error
            .inject_error(),
        ))?;
        write_pipeline.push_write_record(&Arc::new(WriteRecord::new(
            WriteRecordPayload::Single(crate::batch::item::Item::new(
                keyspace.clone(),
                b"b".into(),
                b"b".into(),
                ValueType::Value,
            )),
        )))?;
        write_pipeline.push_write_record(&Arc::new(WriteRecord::new(
            WriteRecordPayload::Single(crate::batch::item::Item::new(
                keyspace.clone(),
                b"c".into(),
                b"c".into(),
                ValueType::Value,
            )),
        )))?;

        // We push a, b, c into the queue using a raw method
        // not the actual write pipeline
        // Then we push d, who becomes leader, this way we simulate a leader
        // being crowned and immediately encountering an error
        assert!(matches!(
            write_pipeline.commit(Arc::new(WriteRecord::new(WriteRecordPayload::Single(
                crate::batch::item::Item::new(
                    keyspace,
                    b"c".into(),
                    b"c".into(),
                    ValueType::Value,
                ),
            )))),
            Err(crate::Error::Io(_))
        ));

        Ok(())
    }

    #[test]
    fn write_pipeline_failure_simple_2() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;
        let db = crate::Database::builder(dir.path()).open()?;
        let keyspace = db.keyspace("items", Default::default)?;
        let write_pipeline = &db.supervisor.write_pipeline;

        let record_a = Arc::new(WriteRecord::new(WriteRecordPayload::Single(
            crate::batch::item::Item::new(
                keyspace.clone(),
                b"a".into(),
                b"a".into(),
                ValueType::Value,
            ),
        )));
        let record_b = Arc::new(WriteRecord::new(WriteRecordPayload::Single(
            crate::batch::item::Item::new(
                keyspace.clone(),
                b"b".into(),
                b"b".into(),
                ValueType::Value,
            ),
        )));
        let record_c = Arc::new(
            WriteRecord::new(WriteRecordPayload::Single(crate::batch::item::Item::new(
                keyspace.clone(),
                b"c".into(),
                b"c".into(),
                ValueType::Value,
            )))
            // Inject I/O error
            .inject_error(),
        );

        write_pipeline.push_write_record(&record_a)?;
        write_pipeline.push_write_record(&record_b)?;
        write_pipeline.push_write_record(&record_c)?;

        assert!(matches!(
            write_pipeline.commit(Arc::new(WriteRecord::new(WriteRecordPayload::Single(
                crate::batch::item::Item::new(
                    keyspace,
                    b"c".into(),
                    b"c".into(),
                    ValueType::Value,
                ),
            )))),
            Err(crate::Error::Io(_))
        ));

        // Leader should still have committed the OK write records
        assert!(record_a.is_published_regular());
        assert!(record_b.is_published_regular());
        assert!(record_c.is_pending());

        Ok(())
    }

    #[test]
    fn write_pipeline_poisoned_leader_does_not_block_later_writers() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;
        let db = crate::Database::builder(dir.path()).open()?;
        let keyspace = db.keyspace("items", Default::default)?;
        let write_pipeline = &db.supervisor.write_pipeline;

        db.is_poisoned.poison();

        let first_record = Arc::new(WriteRecord::new(WriteRecordPayload::Single(
            crate::batch::item::Item::new(
                keyspace.clone(),
                b"first".into(),
                b"value".into(),
                ValueType::Value,
            ),
        )));
        assert!(matches!(
            write_pipeline.commit(first_record),
            Err(crate::Error::Poisoned),
        ));

        let later_record = Arc::new(WriteRecord::new(WriteRecordPayload::Single(
            crate::batch::item::Item::new(
                keyspace,
                b"later".into(),
                b"value".into(),
                ValueType::Value,
            ),
        )));
        assert!(matches!(
            write_pipeline.commit(later_record),
            Err(crate::Error::Poisoned),
        ));

        Ok(())
    }

    #[test]
    fn write_pipeline_poisoned_commits_fill_queue_and_block_later_writers() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;
        let db = crate::Database::builder(dir.path()).open()?;
        let keyspace = db.keyspace("items", Default::default)?;
        let write_pipeline = &db.supervisor.write_pipeline;

        db.is_poisoned.poison();

        for _ in 0..WRITE_PIPELINE_QUEUE_CAPACITY {
            let record = Arc::new(WriteRecord::new(WriteRecordPayload::Single(
                crate::batch::item::Item::new(
                    keyspace.clone(),
                    b"key".into(),
                    b"value".into(),
                    ValueType::Value,
                ),
            )));

            assert!(matches!(
                write_pipeline.commit(record),
                Err(crate::Error::Poisoned),
            ));
        }

        let later_record = Arc::new(WriteRecord::new(WriteRecordPayload::Single(
            crate::batch::item::Item::new(
                keyspace,
                b"later".into(),
                b"value".into(),
                ValueType::Value,
            ),
        )));
        assert!(matches!(
            write_pipeline.commit(later_record),
            Err(crate::Error::Poisoned),
        ));

        Ok(())
    }

    #[test]
    fn write_pipeline_leader_processes_own_record_after_byte_limit() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;
        let db = crate::Database::builder(dir.path())
            .journal_compression(lsm_tree::CompressionType::None)
            .write_pipeline_leader_capacity(64_000)
            .open()?;
        let keyspace = db.keyspace("items", Default::default)?;

        let write_pipeline = &db.supervisor.write_pipeline;

        let preceding_record = Arc::new(WriteRecord::new(WriteRecordPayload::Single(
            crate::batch::item::Item::new(
                keyspace.clone(),
                b"preceding".into(),
                vec![7; 64_000].into(),
                ValueType::Value,
            ),
        )));
        write_pipeline.push_write_record(&preceding_record)?;

        let leader_record = Arc::new(WriteRecord::new(WriteRecordPayload::Single(
            crate::batch::item::Item::new(
                keyspace.clone(),
                b"leader".into(),
                b"value".into(),
                ValueType::Value,
            ),
        )));

        assert!(matches!(
            write_pipeline.commit(leader_record.clone()),
            Ok(Ok(())),
        ));
        assert!(preceding_record.is_published_regular());
        assert!(leader_record.is_published_regular());
        assert!(keyspace.contains_key(b"preceding")?);
        assert!(keyspace.contains_key(b"leader")?);

        Ok(())
    }

    #[test]
    fn write_pipeline_follower_does_not_block_when_leader_errors() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;
        let db = crate::Database::builder(dir.path()).open()?;
        let keyspace = db.keyspace("items", Default::default)?;
        let write_pipeline = &db.supervisor.write_pipeline;

        let leader_record = Arc::new(
            WriteRecord::new(WriteRecordPayload::Single(crate::batch::item::Item::new(
                keyspace.clone(),
                b"leader".into(),
                b"value".into(),
                ValueType::Value,
            )))
            .inject_error(),
        );
        let follower_record = Arc::new(WriteRecord::new(WriteRecordPayload::Single(
            crate::batch::item::Item::new(
                keyspace,
                b"follower".into(),
                b"value".into(),
                ValueType::Value,
            ),
        )));

        write_pipeline.push_write_record(&leader_record)?;
        assert!(write_pipeline.try_acquire_leader_lease());
        write_pipeline.push_write_record(&follower_record)?;

        std::thread::scope(|scope| {
            let follower = scope.spawn(move || write_pipeline.become_follower(follower_record));

            let leader_result = write_pipeline.commit_inner(leader_record, true);
            assert!(matches!(leader_result, Err(crate::Error::Io(_))));

            let Ok(follower_result) = follower.join() else {
                panic!("follower should not panic");
            };
            assert!(matches!(follower_result, Err(crate::Error::Poisoned)));
        });

        Ok(())
    }
}
