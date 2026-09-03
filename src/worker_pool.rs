// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    compaction::worker::run as run_compaction, flush::worker::run as run_flush, poison::PoisonDart,
    stats::Stats, supervisor::Supervisor, Keyspace,
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
    Flush,
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
                Self::Flush => Cow::Borrowed("WorkerMessage:Flush"),
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

    /// Stops every worker and waits for its thread to finish.
    ///
    /// Wakes the workers parked in `rx.recv()` with a `Close` each, then joins
    /// the handles kept since [`WorkerPool::start`]. The join is what makes the
    /// shutdown observable: once this returns, no worker thread is left, so the
    /// database is free to release its resources — the file lock among them.
    ///
    /// Two details are load-bearing, and both are why the previous shutdown
    /// (`while active_thread_counter > 0 { sender.send(Close); sleep }`) could
    /// hang forever:
    ///
    /// * `try_send`, never `send`. A worker busy with a compaction is not
    ///   reading the channel, and the channel is bounded, so a blocking send on
    ///   a full channel waits for a reader that is on its way out — and the
    ///   loop never got back to re-check the counter.
    /// * the queue is drained before every round of wake-ups, so the `Close`
    ///   messages have room even when producers filled it up beforehand.
    ///
    /// Progress is judged by the threads themselves (`is_finished`) rather than
    /// by the counter, so a leaked slot can no longer stall the shutdown.
    pub fn stop_and_join(&self) {
        // A poisoned lock is not a reason to panic here: this runs from
        // `DatabaseInner::drop`, and a panic during another panic's unwind
        // aborts the process. The handles behind the lock are intact whoever
        // panicked while holding it, so take them and go on closing.
        let mut guard = self
            .thread_handles
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let handles = std::mem::take(&mut *guard);
        drop(guard);

        while handles.iter().any(|handle| !handle.is_finished()) {
            // Whatever is still queued will never run: the database is going
            // away. Dropping it now keeps room for the wake-ups.
            let _ = self.rx.drain().count();

            for _ in &handles {
                let _ = self.sender.try_send(WorkerMessage::Close);
            }

            std::thread::sleep(std::time::Duration::from_millis(1));
        }

        for handle in handles {
            // A worker that returned an error has already logged it and
            // poisoned the database; a panicking one is reported by its own
            // hook. Here only the exit matters.
            let _ = handle.join();
        }
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
            log::trace!("acquiring journal lock");
            let journal_writer = keyspace.supervisor.journal.get_writer()?;
            keyspace.inner_rotate_memtable(journal_writer, memtable_id)?;
        }
        WorkerMessage::Flush => {
            let Some(task) = ctx.supervisor.flush_manager.dequeue() else {
                return Ok(false);
            };

            {
                log::trace!("acquiring journal lock to maybe rotate journal");
                let mut journal_writer = ctx.supervisor.journal.get_writer()?;

                if journal_writer.pos()? > 64_000_000 {
                    #[expect(clippy::expect_used)]
                    let mut journal_manager = ctx
                        .supervisor
                        .journal_manager
                        .write()
                        .expect("lock is poisoned");

                    let seqno_map = {
                        #[expect(clippy::expect_used)]
                        let keyspaces = ctx.supervisor.keyspaces.write().expect("lock is poisoned");

                        ctx.supervisor.build_seqno_map(&keyspaces)
                    };

                    journal_manager.rotate_journal(&mut journal_writer, seqno_map)?;

                    if journal_manager.disk_space_used()
                        >= ctx.supervisor.db_config.max_journaling_size_in_bytes
                    {
                        let stragglers =
                            journal_manager.get_keyspaces_to_flush_for_oldest_journal_eviction();

                        for keyspace in stragglers {
                            log::info!(
                                "Rotating {:?} to try to reduce journal size",
                                keyspace.name,
                            );
                            keyspace.request_rotation();
                        }
                    }
                }
            }

            run_flush(
                &task,
                &ctx.supervisor.write_buffer_size,
                &ctx.supervisor.snapshot_tracker,
                &ctx.stats,
            )?;

            for _ in 0..ctx.pool_size {
                ctx.sender
                    .try_send(WorkerMessage::Compact(task.keyspace.clone()))
                    .ok();
            }

            ctx.supervisor
                .journal_manager
                .write()
                .expect("lock is poisoned")
                .maintenance()?;
        }
        WorkerMessage::Compact(keyspace) => {
            // NOTE: Let one worker prioritize flushing if there are pending flushes
            //
            // Disable when only 1 worker exists to avoid deadlock
            if ctx.pool_size > 1 && ctx.worker_id == 0 {
                ctx.sender.send(WorkerMessage::Compact(keyspace)).ok();
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
            let db = Database::builder(&folder).open()?;

            let ks = db.keyspace("default", KeyspaceCreateOptions::default)?;

            ks.insert("a", "a")?;
            ks.rotate_memtable_and_wait()?;

            ks.insert("a", "a")?;
            ks.rotate_memtable_and_wait()?;

            ks.insert("a", "a")?;
            ks.rotate_memtable_and_wait()?;

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

    /// Closing a database whose worker queue is full still finishes.
    ///
    /// Covers the shutdown path that a full queue takes — drain, wake with
    /// `try_send`, join — because that is where the deadlock of
    /// <https://github.com/fjall-rs/fjall/issues/260> lived: `Close` used to go
    /// in with a blocking `send`, and a full queue made it wait for a reader
    /// while the only readers were the workers being stopped.
    ///
    /// It does **not** reproduce that deadlock, and passes on the old shutdown
    /// too: idle workers keep draining the queue, so the send finds room. To
    /// reproduce it, a worker has to be stuck long enough to stop reading —
    /// a compaction blocked on I/O, which a test cannot arrange from the
    /// outside.
    ///
    /// The drop runs on its own thread, so a shutdown that waits forever fails
    /// here on a deadline instead of hanging the whole run.
    #[test]
    fn dropping_a_database_with_a_full_queue_finishes() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;
        let db = Database::builder(&folder).open()?;

        {
            let ks = db.keyspace("default", KeyspaceCreateOptions::default)?;
            ks.insert("a", "a")?;
        }

        // Fill the queue to the brim. Workers keep draining it, so the point is
        // not that it stays full, but that the shutdown meets a full one at
        // least once.
        while db.worker_pool.sender.try_send(WorkerMessage::Flush).is_ok() {}

        let closing = std::thread::spawn(move || drop(db));

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        while !closing.is_finished() {
            assert!(
                std::time::Instant::now() < deadline,
                "closing the database did not finish in 30s: the shutdown is \
                 waiting for a reader of the worker queue",
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert!(closing.join().is_ok(), "closing thread panicked");

        Ok(())
    }

    /// Every worker that is actually spawned holds exactly one slot.
    #[test]
    fn claim_and_spawn_claims_one_slot_per_worker() {
        let counter = Arc::new(AtomicUsize::new(0));

        let handles = claim_and_spawn(3, &counter, |i| Ok::<_, std::io::Error>(i))
            .expect("all spawns succeed");

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
