// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    compaction::worker::run as run_compaction, flush::worker::run as run_flush, poison::PoisonDart,
    stats::Stats, supervisor::Supervisor, AbstractTree, Keyspace,
};
use lsm_tree::MemtableId;
use std::{
    borrow::Cow,
    cell::Cell,
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
                        worker_id: i,
                        rx: self.rx.clone(),
                        supervisor: supervisor.clone(),
                        stats: stats.clone(),
                        next_slot: Cell::new(0),
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
    worker_id: usize,
    supervisor: Supervisor,
    rx: flume::Receiver<WorkerMessage>,
    stats: Arc<Stats>,
    next_slot: Cell<u8>,
}

enum SelectedWork {
    Message(WorkerMessage),
    Flush(Arc<crate::flush::Task>),
    Compact(Keyspace),
}

fn select_work(ctx: &WorkerState) -> Option<SelectedWork> {
    // Rotate among the message queue and both durable work queues. A steady
    // supply of flushes cannot starve rotations, compaction, or Close.
    let start = ctx.next_slot.get();
    for offset in 0..3 {
        let slot = (start + offset) % 3;
        let selected = match slot {
            0 => ctx.rx.try_recv().ok().map(SelectedWork::Message),
            1 => ctx
                .supervisor
                .flush_manager
                .dequeue()
                .map(SelectedWork::Flush),
            _ => ctx
                .supervisor
                .compaction_manager
                .dequeue()
                .map(SelectedWork::Compact),
        };
        if selected.is_some() {
            ctx.next_slot.set((slot + 1) % 3);
            return selected;
        }
    }
    None
}

fn worker_tick(ctx: &WorkerState) -> crate::Result<bool> {
    let selected = if let Some(selected) = select_work(ctx) {
        selected
    } else {
        let Ok(message) = ctx.rx.recv() else {
            return Ok(true);
        };
        ctx.next_slot.set(1);
        SelectedWork::Message(message)
    };

    log::trace!("Worker #{} selected work", ctx.worker_id);

    match selected {
        SelectedWork::Message(WorkerMessage::Close) => return Ok(true),
        SelectedWork::Message(WorkerMessage::RotateMemtable(keyspace, memtable_id)) => {
            log::trace!("acquiring journal lock");
            let journal_writer = keyspace.supervisor.journal.get_writer()?;
            keyspace.inner_rotate_memtable(journal_writer, memtable_id)?;
        }
        SelectedWork::Flush(task) => flush_work(ctx, &task)?,
        SelectedWork::Message(WorkerMessage::Flush) => {
            if let Some(task) = ctx.supervisor.flush_manager.dequeue() {
                flush_work(ctx, &task)?;
            }
        }
        SelectedWork::Compact(keyspace) => handle_compaction(ctx, &keyspace)?,
        SelectedWork::Message(WorkerMessage::Compact(_)) => {
            if let Some(keyspace) = ctx.supervisor.compaction_manager.dequeue() {
                handle_compaction(ctx, &keyspace)?;
            }
        }
    }

    Ok(false)
}

fn flush_work(ctx: &WorkerState, task: &crate::flush::Task) -> crate::Result<()> {
    {
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
                for keyspace in journal_manager.get_keyspaces_to_flush_for_oldest_journal_eviction()
                {
                    keyspace.request_rotation();
                }
            }
        } else {
            // https://github.com/fjall-rs/fjall/issues/329
            journal_writer.persist(crate::PersistMode::SyncAll)?;
        }
    }
    run_flush(
        task,
        &ctx.supervisor.write_buffer_size,
        &ctx.supervisor.snapshot_tracker,
        &ctx.stats,
    )?;
    if task.keyspace.tree.sealed_memtable_count() > 0
        && ctx
            .supervisor
            .flush_manager
            .enqueue(Arc::new(crate::flush::Task {
                keyspace: task.keyspace.clone(),
            }))
    {
        task.keyspace
            .worker_messager
            .try_send(WorkerMessage::Flush)
            .ok();
    }
    ctx.supervisor.request_compaction(&task.keyspace);
    #[expect(clippy::expect_used)]
    ctx.supervisor
        .journal_manager
        .write()
        .expect("lock is poisoned")
        .maintenance()?;
    Ok(())
}

fn handle_compaction(ctx: &WorkerState, keyspace: &Keyspace) -> crate::Result<()> {
    let before = keyspace.tree.l0_run_count();
    run_compaction(keyspace, &ctx.supervisor.snapshot_tracker, &ctx.stats)?;
    let after = keyspace.tree.l0_run_count();
    // Continue draining an L0 backlog only while this worker makes progress.
    // A strategy with no eligible task must not spin forever on one keyspace.
    if after >= 20 && after < before {
        ctx.supervisor.request_compaction(keyspace);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AbstractTree, Database, KeyspaceCreateOptions};
    use test_log::test;

    /// Fill the queue before rotation: no probabilistic producer race is needed.
    /// The child keeps a deadlocked database from hanging the whole test suite.
    #[test]
    fn rotation_does_not_wait_for_worker_queue_capacity() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;
        if std::env::var_os("FJALL_SATURATED_ROTATION_CHILD").is_none() {
            let executable = std::env::current_exe()?;
            let mut child = std::process::Command::new(executable)
                .args([
                    "--exact",
                    "worker_pool::tests::rotation_does_not_wait_for_worker_queue_capacity",
                ])
                .env("FJALL_SATURATED_ROTATION_CHILD", "1")
                .env("FJALL_SATURATED_ROTATION_DIR", folder.path())
                .spawn()?;
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
            loop {
                if let Some(status) = child.try_wait()? {
                    assert!(
                        status.success(),
                        "saturated rotation child failed: {status}"
                    );
                    let reopened = Database::builder(folder.path()).worker_threads(1).open()?;
                    let recovered =
                        reopened.keyspace("saturated", KeyspaceCreateOptions::default)?;
                    let Some(value) = recovered.get("key")? else {
                        panic!("committed key missing after reopening");
                    };
                    assert_eq!(&*value, b"value");
                    return Ok(());
                }
                if std::time::Instant::now() >= deadline {
                    child.kill()?;
                    child.wait()?;
                    panic!("rotation blocked on its own full worker queue");
                }
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
        }

        let Ok(path) = std::env::var("FJALL_SATURATED_ROTATION_DIR") else {
            panic!("saturated rotation child is missing its directory");
        };
        let db = Database::builder(&path)
            .worker_threads_unchecked(0)
            .open()?;
        let ks = db.keyspace("saturated", KeyspaceCreateOptions::default)?;
        ks.insert("key", "value")?;
        let id = ks.tree.active_memtable().id();
        for _ in 0..1_000 {
            assert!(db
                .worker_pool
                .sender
                .try_send(WorkerMessage::RotateMemtable(ks.clone(), id))
                .is_ok());
        }
        assert!(ks.rotate_memtable()?);
        assert_eq!(ks.tree.sealed_memtable_count(), 1);

        // The flush notification could not be queued. Existing messages must
        // still lead workers to discover and execute the pending flush task.
        let ctx = WorkerState {
            worker_id: 0,
            supervisor: db.supervisor.clone(),
            rx: db.worker_pool.rx.clone(),
            stats: db.stats.clone(),
            next_slot: Cell::new(1),
        };
        worker_tick(&ctx)?;
        assert_eq!(ks.tree.sealed_memtable_count(), 0);
        let Some(value) = ks.get("key")? else {
            panic!("key missing after flushing");
        };
        assert_eq!(&*value, b"value");

        // Repeated requests for one keyspace occupy only one pending slot.
        let task = Arc::new(crate::flush::Task {
            keyspace: ks.clone(),
        });
        for _ in 0..1_001 {
            db.supervisor.flush_manager.enqueue(task.clone());
        }
        assert_eq!(db.supervisor.flush_manager.len(), 1);
        db.supervisor.flush_manager.clear();

        // A full message queue cannot discard the compaction request.
        assert_eq!(db.worker_pool.sender.len(), 1_000);
        db.supervisor.request_compaction(&ks);
        ctx.next_slot.set(2);
        assert!(!worker_tick(&ctx)?);
        assert!(db.supervisor.compaction_manager.dequeue().is_none());
        db.persist(crate::PersistMode::SyncAll)?;
        // The zero-worker test database has messages retaining keyspace Arcs;
        // exit directly rather than invoking its destructor in this child.
        std::process::exit(0);
    }

    #[test]
    fn pending_flush_and_compaction_cannot_starve_close() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;
        let db = Database::builder(&folder)
            .worker_threads_unchecked(0)
            .open()?;
        let ks = db.keyspace("fair", KeyspaceCreateOptions::default)?;
        let ctx = WorkerState {
            worker_id: 0,
            supervisor: db.supervisor.clone(),
            rx: db.worker_pool.rx.clone(),
            stats: db.stats.clone(),
            next_slot: Cell::new(1),
        };
        assert!(db.worker_pool.sender.send(WorkerMessage::Close).is_ok());
        db.supervisor
            .flush_manager
            .enqueue(Arc::new(crate::flush::Task {
                keyspace: ks.clone(),
            }));
        db.supervisor.request_compaction(&ks);
        assert!(!worker_tick(&ctx)?);
        assert!(!worker_tick(&ctx)?);
        assert!(
            worker_tick(&ctx)?,
            "Close should be served in one scheduling cycle"
        );
        // The zero-worker database still has hints holding keyspace Arcs.
        db.supervisor.flush_manager.clear();
        db.supervisor.compaction_manager.clear();
        while db.worker_pool.rx.try_recv().is_ok() {}
        Ok(())
    }

    #[test]
    fn recreated_keyspace_keeps_distinct_pending_work() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;
        let db = Database::builder(&folder)
            .worker_threads_unchecked(0)
            .open()?;
        let old = db.keyspace("reused", KeyspaceCreateOptions::default)?;
        db.delete_keyspace(old.clone())?;
        let new = db.keyspace("reused", KeyspaceCreateOptions::default)?;
        assert_ne!(old.id(), new.id());

        for keyspace in [&old, &new] {
            assert!(db
                .supervisor
                .flush_manager
                .enqueue(Arc::new(crate::flush::Task {
                    keyspace: keyspace.clone(),
                })));
            assert!(db.supervisor.compaction_manager.enqueue(keyspace.clone()));
        }
        assert_eq!(db.supervisor.flush_manager.len(), 2);
        assert_eq!(
            db.supervisor
                .flush_manager
                .dequeue()
                .map(|task| task.keyspace.id()),
            Some(old.id())
        );
        assert_eq!(
            db.supervisor
                .flush_manager
                .dequeue()
                .map(|task| task.keyspace.id()),
            Some(new.id())
        );
        assert_eq!(
            db.supervisor
                .compaction_manager
                .dequeue()
                .map(|keyspace| keyspace.id()),
            Some(old.id())
        );
        assert_eq!(
            db.supervisor
                .compaction_manager
                .dequeue()
                .map(|keyspace| keyspace.id()),
            Some(new.id())
        );
        while db.worker_pool.rx.try_recv().is_ok() {}
        Ok(())
    }

    #[test]
    fn full_message_queue_does_not_lose_l0_compaction() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;
        let db = Database::builder(&folder)
            .worker_threads_unchecked(0)
            .open()?;
        let ks = db.keyspace("compact", KeyspaceCreateOptions::default)?;
        for row in 0..6 {
            ks.insert(b"key", format!("value-{row}").as_bytes())?;
            db.persist(crate::PersistMode::SyncAll)?;
            assert!(ks.rotate_memtable()?);
            run_flush(
                &crate::flush::Task {
                    keyspace: ks.clone(),
                },
                &db.supervisor.write_buffer_size,
                &db.supervisor.snapshot_tracker,
                &db.stats,
            )?;
            db.supervisor.flush_manager.clear();
        }
        let before = ks.tree.l0_run_count();
        assert!(before >= 4, "expected four L0 runs, found {before}");
        while db.worker_pool.sender.len() < 1_000 {
            assert!(db.worker_pool.sender.try_send(WorkerMessage::Flush).is_ok());
        }
        db.supervisor.request_compaction(&ks);
        let ctx = WorkerState {
            worker_id: 0,
            supervisor: db.supervisor.clone(),
            rx: db.worker_pool.rx.clone(),
            stats: db.stats.clone(),
            next_slot: Cell::new(2),
        };
        assert!(!worker_tick(&ctx)?);
        assert!(ks.tree.l0_run_count() < before);
        assert!(db.supervisor.compaction_manager.dequeue().is_none());
        while db.worker_pool.rx.try_recv().is_ok() {}
        Ok(())
    }

    /// A write stalled at 30 L0 runs must request work even when an earlier
    /// compaction chose another level and left the L0 count unchanged.
    #[test]
    fn halted_writer_requests_compaction_again() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;
        if std::env::var_os("FJALL_HALTED_WRITER_CHILD").is_none() {
            let mut child = std::process::Command::new(std::env::current_exe()?)
                .args([
                    "--exact",
                    "worker_pool::tests::halted_writer_requests_compaction_again",
                ])
                .env("FJALL_HALTED_WRITER_CHILD", "1")
                .env("FJALL_HALTED_WRITER_DIR", folder.path())
                .spawn()?;
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
            loop {
                if let Some(status) = child.try_wait()? {
                    assert!(
                        status.success(),
                        "halted writer did not request compaction twice: {status}"
                    );
                    return Ok(());
                }
                if std::time::Instant::now() >= deadline {
                    child.kill()?;
                    child.wait()?;
                    panic!("halted writer did not request compaction before deadline");
                }
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
        }

        let Ok(path) = std::env::var("FJALL_HALTED_WRITER_DIR") else {
            panic!("halted writer child is missing its directory");
        };
        let db = Database::builder(&path)
            .worker_threads_unchecked(0)
            .open()?;
        let ks = db.keyspace("halted", KeyspaceCreateOptions::default)?;
        for row in 0..30 {
            ks.insert(b"key", format!("value-{row}").as_bytes())?;
            db.persist(crate::PersistMode::SyncAll)?;
            assert!(ks.rotate_memtable()?);
            run_flush(
                &crate::flush::Task {
                    keyspace: ks.clone(),
                },
                &db.supervisor.write_buffer_size,
                &db.supervisor.snapshot_tracker,
                &db.stats,
            )?;
            db.supervisor.flush_manager.clear();
        }
        assert!(ks.tree.l0_run_count() >= 30);
        db.supervisor.compaction_manager.clear();
        let writer = ks.clone();
        std::thread::spawn(move || writer.local_backpressure());
        for request in 0..2 {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
            loop {
                assert!(ks.tree.l0_run_count() >= 30);
                if db.supervisor.compaction_manager.dequeue().is_some() {
                    break;
                }
                if std::time::Instant::now() >= deadline {
                    std::process::exit(2 + request);
                }
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
        }
        std::process::exit(0);
    }

    /// Concurrent writes across keyspaces exercise rotations and flushes with
    /// the pool sizes that previously differed in the field investigation.
    #[test]
    fn concurrent_rotations_preserve_values_and_deletions() -> crate::Result<()> {
        for worker_count in [1, 2, 4] {
            let folder = tempfile::tempdir()?;
            let options = || KeyspaceCreateOptions::default().max_memtable_size(2_048);
            {
                let db = Database::builder(&folder)
                    .worker_threads(worker_count)
                    .open()?;
                let left = db.keyspace("left", options)?;
                let right = db.keyspace("right", options)?;
                let handles: Vec<_> = (0..4)
                    .map(|writer| {
                        let left = left.clone();
                        let right = right.clone();
                        std::thread::spawn(move || -> crate::Result<()> {
                            for row in 0..500 {
                                let key = format!("key-{writer}-{row}");
                                let value = format!("value-{writer}-{row}");
                                let target = if row % 2 == 0 { &left } else { &right };
                                target.insert(key.as_bytes(), value.as_bytes())?;
                                if row % 7 == 0 {
                                    target.remove(key.as_bytes())?;
                                }
                            }
                            Ok(())
                        })
                    })
                    .collect();
                for handle in handles {
                    let Ok(result) = handle.join() else {
                        panic!("writer thread panicked");
                    };
                    result?;
                }
                db.persist(crate::PersistMode::SyncAll)?;
                left.rotate_memtable_and_wait()?;
                right.rotate_memtable_and_wait()?;
                let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
                while left.tree.sealed_memtable_count() > 0
                    || right.tree.sealed_memtable_count() > 0
                {
                    assert!(std::time::Instant::now() < deadline, "flush did not finish");
                    std::thread::sleep(std::time::Duration::from_millis(10));
                }
                assert_eq!(left.tree.sealed_memtable_count(), 0);
                assert_eq!(right.tree.sealed_memtable_count(), 0);
            }

            let db = Database::builder(&folder)
                .worker_threads(worker_count)
                .open()?;
            let left = db.keyspace("left", options)?;
            let right = db.keyspace("right", options)?;
            for writer in 0..4 {
                for row in 0..500 {
                    let key = format!("key-{writer}-{row}");
                    let target = if row % 2 == 0 { &left } else { &right };
                    let found = target.get(key.as_bytes())?;
                    if row % 7 == 0 {
                        assert!(found.is_none(), "deleted key survived reopen: {key}");
                    } else {
                        let Some(value) = found else {
                            panic!("key missing after reopen: {key}");
                        };
                        assert_eq!(&*value, format!("value-{writer}-{row}").as_bytes());
                    }
                }
            }
        }
        Ok(())
    }

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
