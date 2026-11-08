// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    compaction::worker::run as run_compaction, poison::PoisonDart, stats::Stats,
    supervisor::Supervisor, Keyspace,
};
use lsm_tree::MemtableId;
use std::{
    borrow::Cow,
    sync::{
        atomic::{AtomicUsize, Ordering::Relaxed},
        Arc, Mutex,
    },
    thread::JoinHandle,
};

pub enum WorkerMessage {
    Flush(Keyspace),
    Compact(Keyspace),
    Close,
    RotateMemtable(Keyspace, MemtableId),
}

impl std::fmt::Debug for WorkerMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::Flush(k) => Cow::Owned(format!("WorkerMessage:Flush({:?})", k.name)),
                Self::Compact(k) => Cow::Owned(format!("WorkerMessage:Compact({:?})", k.name)),
                Self::Close => Cow::Borrowed("WorkerMessage:Close"),
                Self::RotateMemtable(k, memtable_id) =>
                    Cow::Owned(format!("WorkerMessage:Rotate({:?}, {memtable_id})", k.name)),
            }
        )
    }
}

type WorkerHandle = JoinHandle<Result<(), crate::Error>>;

pub struct WorkerPool {
    thread_handles: Mutex<Vec<WorkerHandle>>,
    pub(crate) rx: flume::Receiver<WorkerMessage>,
    pub(crate) sender: flume::Sender<WorkerMessage>,
}

impl WorkerPool {
    pub fn prepare() -> Self {
        let (sender, rx) = flume::bounded(1_000);

        Self {
            thread_handles: Mutex::default(),
            rx,
            sender,
        }
    }

    pub fn start(
        &self,
        pool_size: usize,
        supervisor: &Supervisor,
        stats: &Arc<Stats>,
        poison_dart: &PoisonDart,
        thread_counter: &Arc<AtomicUsize>,
    ) -> crate::Result<()> {
        log::debug!("Starting worker pool with {pool_size} threads");

        let thread_handles = claim_and_spawn(pool_size, thread_counter, |i| {
            std::thread::Builder::new()
                .name("fjall:worker".to_string())
                .spawn({
                    log::trace!("Starting fjall worker thread #{i}");

                    let worker_state = WorkerState {
                        pool_size,
                        worker_id: i,
                        rx: self.rx.clone(),
                        supervisor: supervisor.clone(),
                        stats: stats.clone(),
                        sender: self.sender.clone(),
                    };

                    let thread_counter = thread_counter.clone();
                    let poison_dart = poison_dart.clone();

                    move || {
                        // The counter must drop on *every* way out of this
                        // thread, not just the graceful one: `Database::drop`
                        // spins on it (`while counter > 0`), so a worker that
                        // returns an error or unwinds would keep the database
                        // closing forever.
                        let _counter_guard = ActiveThreadGuard(thread_counter);

                        loop {
                            match worker_tick(&worker_state) {
                                Ok(should_abort) => {
                                    if should_abort {
                                        log::debug!("Worker #{i} closes because DB is dropping");
                                        return Ok(());
                                    }
                                }
                                Err(e) => {
                                    log::error!("Worker #{i} crashed: {e:?}");
                                    poison_dart.poison();
                                    return Err(e);
                                }
                            }
                        }
                    }
                })
        })?;

        *self.thread_handles.lock().expect("lock is poisoned") = thread_handles;

        Ok(())
    }
}

/// Claims one slot in the active thread counter per worker, immediately before
/// that worker is spawned, and hands the slot straight back if the spawn fails.
///
/// Claiming the whole pool up front would leak the slots of the workers that failed
/// spawn never reaches: nothing ever decrements them, because those threads do
/// not exist, and `DatabaseInner::drop` waits for the counter to reach zero.
///
/// The spawn is a parameter so the failure path can be tested without having to
/// exhaust the operating system's thread limit.
fn claim_and_spawn<H, S: FnMut(usize) -> std::io::Result<H>>(
    pool_size: usize,
    thread_counter: &Arc<AtomicUsize>,
    mut spawn: S,
) -> std::io::Result<Vec<H>> {
    (0..pool_size)
        .map(|i| {
            thread_counter.fetch_add(1, Relaxed);

            spawn(i).inspect_err(|_| {
                thread_counter.fetch_sub(1, Relaxed);
            })
        })
        .collect()
}

/// Decrements the pool's active thread counter when a worker thread leaves,
/// whatever the reason: graceful close, error return or unwinding panic.
///
/// `DatabaseInner::drop` waits for this counter to reach zero, so a leaked
/// increment makes closing the database hang forever.
struct ActiveThreadGuard(Arc<AtomicUsize>);

impl Drop for ActiveThreadGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Relaxed);
    }
}

struct WorkerState {
    pool_size: usize,
    worker_id: usize,
    supervisor: Supervisor,
    rx: flume::Receiver<WorkerMessage>,
    sender: flume::Sender<WorkerMessage>,
    stats: Arc<Stats>,
}

fn worker_tick(ctx: &WorkerState) -> crate::Result<bool> {
    let Ok(item) = ctx.rx.recv() else {
        return Ok(true);
    };

    log::trace!("Worker #{} got message: {item:?}", ctx.worker_id);

    match item {
        WorkerMessage::Close => {
            return Ok(true);
        }
        WorkerMessage::RotateMemtable(keyspace, memtable_id) => {
            log::trace!("acquiring ledger lock");

            let ledger = ctx.supervisor.ledger()?;

            keyspace.inner_rotate_memtable(ledger, memtable_id)?;
        }
        WorkerMessage::Flush(keyspace) => {
            match keyspace.flush(false) {
                // NOTE: The keyspace was deleted after the flush was queued
                Err(crate::Error::KeyspaceDeleted) => {}
                result => result?,
            }

            for _ in 0..ctx.pool_size {
                ctx.sender
                    .try_send(WorkerMessage::Compact(keyspace.clone()))
                    .ok();
            }
        }
        WorkerMessage::Compact(keyspace) => {
            // NOTE: Let one worker prioritize flushing if there are pending flushes
            if ctx.pool_size > 1 && ctx.worker_id == 0 {
                ctx.sender.try_send(WorkerMessage::Compact(keyspace)).ok();
                return Ok(false);
            }

            run_compaction(&keyspace, &ctx.supervisor.snapshot_tracker, &ctx.stats)?;
        }
    }

    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AbstractTree, Database, KeyspaceCreateOptions};
    use test_log::test;

    // https://github.com/fjall-rs/fjall/pull/303
    #[test]
    fn keyspace_compact_after_startup() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;

        {
            let db = Database::builder(&folder)
                .worker_threads_unchecked(0)
                .open()?;

            let ks = db.keyspace("default", KeyspaceCreateOptions::default)?;

            ks.insert("a", "a", crate::DurabilityLevel::Buffer)?;
            ks.flush(true)?;

            ks.insert("a", "a", crate::DurabilityLevel::Buffer)?;
            ks.flush(true)?;

            ks.insert("a", "a", crate::DurabilityLevel::Buffer)?;
            ks.flush(true)?;

            assert!(ks.tree.l0_run_count() > 0);
        }

        {
            let db = Database::builder(&folder)
                .worker_threads_unchecked(0)
                .open()?;

            assert_eq!(
                1,
                db.worker_pool.rx.len(),
                "worker message should be enqueued on startup",
            );
            let item = db.worker_pool.rx.try_recv().expect("should get message");
            assert!(
                matches!(item, WorkerMessage::Compact(_)),
                "worker message should be compaction request",
            );
        }

        Ok(())
    }

    /// Runs the worker messages of a database without worker threads, like one worker would.
    fn run_worker_messages(db: &Database) -> crate::Result<()> {
        let ctx = WorkerState {
            pool_size: 1,
            worker_id: 0,
            supervisor: db.supervisor.clone(),
            rx: db.worker_pool.rx.clone(),
            sender: db.worker_pool.sender.clone(),
            stats: db.stats.clone(),
        };

        while !ctx.rx.is_empty() {
            worker_tick(&ctx)?;
        }

        Ok(())
    }

    /// A rotation request seals the active memtable, and the worker flushes it in the background.
    #[test]
    fn worker_rotates_and_flushes_memtable() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;

        {
            let db = Database::builder(&folder)
                .worker_threads_unchecked(0)
                .open()?;

            let ks = db.keyspace("default", KeyspaceCreateOptions::default)?;
            ks.insert("a", "a", crate::DurabilityLevel::Buffer)?;
            ks.insert("b", "b", crate::DurabilityLevel::Buffer)?;

            ks.request_rotation();
            run_worker_messages(&db)?;

            assert_eq!(1, ks.tree.table_count(), "memtable should be flushed");
            assert_eq!(0, ks.sealed_memtable_count());
            assert!(ks.tree.active_memtable().is_empty());
            assert_eq!(Some(1), ks.tree.get_flush_watermark());
            assert_eq!(0, db.supervisor.write_buffer_size.get());
            assert_eq!(Some("a".as_bytes().into()), ks.get("a")?);
        }

        {
            let db = Database::builder(&folder)
                .worker_threads_unchecked(0)
                .open()?;

            let ks = db.keyspace("default", KeyspaceCreateOptions::default)?;

            assert_eq!(1, ks.tree.table_count());
            assert_eq!(Some("a".as_bytes().into()), ks.get("a")?);
            assert_eq!(Some("b".as_bytes().into()), ks.get("b")?);
        }

        Ok(())
    }

    /// A flush request of a cleared keyspace does nothing (its sealed memtables are gone).
    #[test]
    fn worker_flush_after_clear_does_nothing() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;

        let db = Database::builder(&folder)
            .worker_threads_unchecked(0)
            .open()?;

        let ks = db.keyspace("default", KeyspaceCreateOptions::default)?;
        ks.insert("a", "a", crate::DurabilityLevel::Buffer)?;

        // NOTE: Seals the memtable and queues the flush, without running it
        {
            let ctx_rx = db.worker_pool.rx.clone();
            ks.request_rotation();
            let msg = ctx_rx.try_recv().expect("should have rotation request");
            let WorkerMessage::RotateMemtable(ks, memtable_id) = msg else {
                panic!("should be rotation request");
            };
            ks.inner_rotate_memtable(db.supervisor.ledger()?, memtable_id)?;
        }
        assert_eq!(1, ks.sealed_memtable_count());

        ks.clear()?;
        run_worker_messages(&db)?;

        assert_eq!(0, ks.tree.table_count());
        assert_eq!(0, ks.sealed_memtable_count());
        assert!(ks.is_empty()?);

        Ok(())
    }

    /// Every worker that is actually spawned holds exactly one slot.
    #[test]
    fn claim_and_spawn_claims_one_slot_per_worker() {
        let counter = Arc::new(AtomicUsize::new(0));

        let handles = claim_and_spawn(3, &counter, Ok).expect("all spawns succeed");

        assert_eq!(handles, vec![0, 1, 2]);
        assert_eq!(counter.load(Relaxed), 3);
    }

    /// A failed spawn releases its own slot and never claims one for the workers
    /// it did not get to. `DatabaseInner::drop` spins until the counter reaches
    /// zero, so a slot held by a thread that does not exist hangs the close
    /// forever.
    #[test]
    fn claim_and_spawn_counts_live_workers_only() {
        let counter = Arc::new(AtomicUsize::new(0));

        let result = claim_and_spawn(4, &counter, |i| {
            if i == 2 {
                Err(std::io::Error::other("cannot spawn thread"))
            } else {
                Ok(i)
            }
        });

        assert!(result.is_err());
        assert_eq!(
            counter.load(Relaxed),
            2,
            "only the two workers that started should hold a slot",
        );
    }

    /// A worker leaving normally releases its slot in the counter.
    #[test]
    fn active_thread_guard_decrements_on_scope_exit() {
        let counter = Arc::new(AtomicUsize::new(1));
        {
            let _guard = ActiveThreadGuard(counter.clone());
            assert_eq!(counter.load(Relaxed), 1);
        }
        assert_eq!(counter.load(Relaxed), 0);
    }

    /// A worker that returns an error releases its slot too: `Database::drop`
    /// spins until the counter reaches zero, so a leaked slot would hang the
    /// close forever.
    #[test]
    fn active_thread_guard_decrements_on_early_return() {
        let counter = Arc::new(AtomicUsize::new(1));

        fn failing_worker(counter: Arc<AtomicUsize>) -> Result<(), ()> {
            let _guard = ActiveThreadGuard(counter);
            Err(())
        }

        assert!(failing_worker(counter.clone()).is_err());
        assert_eq!(counter.load(Relaxed), 0);
    }

    /// A panicking worker releases its slot as well.
    #[test]
    fn active_thread_guard_decrements_on_panic() {
        let counter = Arc::new(AtomicUsize::new(1));
        let counter_in_thread = counter.clone();

        let outcome = std::thread::spawn(move || {
            let _guard = ActiveThreadGuard(counter_in_thread);
            panic!("worker crashed");
        })
        .join();

        assert!(outcome.is_err());
        assert_eq!(counter.load(Relaxed), 0);
    }
}
