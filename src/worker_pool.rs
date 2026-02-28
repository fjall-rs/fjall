// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    compaction::worker::run as run_compaction, flush::worker::run as run_flush,
    journal::manager::EvictionWatermark, poison_dart::PoisonDart, stats::Stats,
    supervisor::Supervisor, Keyspace,
};
use lsm_tree::{AbstractTree, MemtableId};
use std::{
    borrow::Cow,
    sync::{atomic::AtomicUsize, Arc, Mutex},
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
        use std::sync::atomic::Ordering::Relaxed;

        log::debug!("Starting worker pool with {pool_size} threads");

        thread_counter.fetch_add(pool_size, Relaxed);

        let thread_handles = (0..pool_size)
            .map(|i| {
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

                        move || loop {
                            match worker_tick(&worker_state) {
                                Ok(should_abort) => {
                                    if should_abort {
                                        log::debug!("Worker #{i} closes because DB is dropping");
                                        thread_counter.fetch_sub(1, Relaxed);
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
                    })
                    .inspect_err(|_| {
                        thread_counter.fetch_sub(1, Relaxed);
                    })
            })
            .collect::<Result<_, _>>()?;

        *self.thread_handles.lock().expect("lock is poisoned") = thread_handles;

        Ok(())
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
            let journal_writer = keyspace.supervisor.journal.get_writer();
            keyspace.inner_rotate_memtable(journal_writer, memtable_id)?;
        }
        WorkerMessage::Flush => {
            let Some(task) = ctx.supervisor.flush_manager.dequeue() else {
                return Ok(false);
            };

            {
                log::trace!("acquiring journal lock to maybe rotate journal");
                let mut journal_writer = ctx.supervisor.journal.get_writer();

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

                        let mut seqnos = Vec::with_capacity(keyspaces.len());

                        for keyspace in keyspaces.values() {
                            if let Some(lsn) = keyspace.tree.get_highest_memtable_seqno() {
                                seqnos.push(EvictionWatermark {
                                    lsn,
                                    keyspace: keyspace.clone(),
                                });
                            }
                        }

                        seqnos
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
                                keyspace.name
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
