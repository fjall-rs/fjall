// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    compaction::worker::run as run_compaction,
    flush::{worker::run as run_flush, Task as FlushTask},
    poison::PoisonDart,
    stats::Stats,
    supervisor::Supervisor,
    Keyspace,
};
use lsm_tree::AbstractTree;
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering::Relaxed},
        Arc, Mutex,
    },
    thread::JoinHandle,
};

type WorkerHandle = JoinHandle<Result<(), crate::Error>>;

pub struct WorkerPool {
    thread_handles: Mutex<Vec<WorkerHandle>>,
    /// One slot. Extra pokes while a worker is already awake are dropped.
    pub(crate) wake: flume::Sender<()>,
    wake_rx: flume::Receiver<()>,
}

impl WorkerPool {
    pub fn prepare() -> Self {
        let (wake, wake_rx) = flume::bounded(1);

        Self {
            thread_handles: Mutex::default(),
            wake,
            wake_rx,
        }
    }

    pub(crate) fn poke(&self) {
        self.wake.try_send(()).ok();
    }

    pub fn start(
        &self,
        pool_size: usize,
        supervisor: &Supervisor,
        stats: &Arc<Stats>,
        poison_dart: &PoisonDart,
        thread_counter: &Arc<AtomicUsize>,
        stop: &lsm_tree::stop_signal::StopSignal,
    ) -> crate::Result<()> {
        log::debug!("Starting worker pool with {pool_size} threads");

        let thread_handles = claim_and_spawn(pool_size, thread_counter, |i| {
            std::thread::Builder::new()
                .name("fjall:worker".to_string())
                .spawn({
                    log::trace!("Starting fjall worker thread #{i}");

                    let worker_state = WorkerState {
                        wake: self.wake.clone(),
                        wake_rx: self.wake_rx.clone(),
                        supervisor: supervisor.clone(),
                        stats: stats.clone(),
                        stop: stop.clone(),
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
    supervisor: Supervisor,
    wake: flume::Sender<()>,
    wake_rx: flume::Receiver<()>,
    stats: Arc<Stats>,
    stop: lsm_tree::stop_signal::StopSignal,
}

impl WorkerState {
    fn poke(&self) {
        self.wake.try_send(()).ok();
    }
}

fn worker_tick(ctx: &WorkerState) -> crate::Result<bool> {
    if ctx.stop.is_stopped() {
        return Ok(true);
    }

    if let Some(task) = ctx.supervisor.flush_manager.dequeue() {
        ctx.poke();
        process_one_flush(ctx, task.as_ref())?;
        return Ok(false);
    }

    if run_pending_keyspace_work(ctx)? {
        ctx.poke();
        return Ok(false);
    }

    if ctx.stop.is_stopped() {
        return Ok(true);
    }

    Ok(ctx.wake_rx.recv().is_err())
}

fn run_pending_keyspace_work(ctx: &WorkerState) -> crate::Result<bool> {
    let keyspaces: Vec<Keyspace> = ctx
        .supervisor
        .keyspaces
        .read()
        .expect("lock is poisoned")
        .values()
        .cloned()
        .collect();

    for keyspace in keyspaces {
        if keyspace.take_rotate_pending() {
            log::trace!("acquiring journal lock");
            let journal_writer = keyspace.supervisor.journal.get_writer()?;
            let memtable_id = keyspace.tree.active_memtable().id();
            keyspace.inner_rotate_memtable(journal_writer, memtable_id)?;
            return Ok(true);
        }

        if keyspace.take_compact_pending() {
            run_compaction(&keyspace, &ctx.supervisor.snapshot_tracker, &ctx.stats)?;
            return Ok(true);
        }
    }

    Ok(false)
}

fn process_one_flush(ctx: &WorkerState, task: &FlushTask) -> crate::Result<()> {
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
                    log::info!("Rotating {:?} to try to reduce journal size", keyspace.name,);
                    keyspace.request_rotation();
                }
            }
        }
    }

    run_flush(
        task,
        &ctx.supervisor.write_buffer_size,
        &ctx.supervisor.snapshot_tracker,
        &ctx.stats,
    )?;

    task.keyspace.request_compact();

    ctx.supervisor
        .journal_manager
        .write()
        .expect("lock is poisoned")
        .maintenance()?;

    Ok(())
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

            let ks = db.keyspace("default", KeyspaceCreateOptions::default)?;
            assert!(
                ks.compact_is_pending(),
                "compaction should be pending on startup when L0 exists",
            );
        }

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
