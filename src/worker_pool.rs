use crate::{
    compaction::worker::run as run_compaction, flush::worker::run as flush_memtable, stats::Stats,
    supervisor::Supervisor,
};
use std::{
    sync::{Arc, Mutex},
    thread::JoinHandle,
};

pub enum WorkerMessage {
    Flush,
    Compact,
}

impl std::fmt::Debug for WorkerMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                // Self::RotateMemtable(keyspace) => format!("Rotate({})", keyspace.name),
                Self::Flush => "WorkerMessage:Flush",
                Self::Compact => "WorkerMessage:Compact",
            }
        )
    }
}

type WorkerHandle = JoinHandle<Result<(), crate::Error>>;

pub struct WorkerPoolInner {
    thread_handles: Vec<WorkerHandle>,
    lock: Arc<Mutex<()>>,
}

impl WorkerPoolInner {
    pub fn new(
        thread_count: usize,
        supervisor: &Supervisor,
        stats: &Arc<Stats>,
    ) -> crate::Result<(Self, flume::Sender<WorkerMessage>)> {
        let (message_queue_sender, rx) = flume::bounded(100_000);
        let lock = Arc::new(Mutex::default());

        let thread_handles = (0..thread_count)
            .map(|i| {
                log::debug!("Starting fjall worker thread #{i}");

                let worker_state = WorkerState {
                    worker_id: i,
                    rx: rx.clone(),
                    supervisor: supervisor.clone(),
                    stats: stats.clone(),
                    lock: lock.clone(),
                    sender: message_queue_sender.clone(),
                };

                std::thread::Builder::new()
                    .name("fjallworker".to_string())
                    .spawn(move || {
                        loop {
                            match worker_tick(&worker_state) {
                                Ok(should_abort) => {
                                    if should_abort {
                                        return Ok(());
                                    }
                                }
                                Err(e) => {
                                    log::error!("Worker #{i} crashed: {e:?}");
                                    // TODO: poison DB
                                    return Err(e);
                                }
                            }
                        }
                    })
            })
            .collect::<Result<_, _>>()?;

        Ok((
            Self {
                thread_handles,
                lock,
            },
            message_queue_sender,
        ))
    }
}

#[derive(Clone)]
pub struct WorkerPool(Arc<WorkerPoolInner>);

impl WorkerPool {
    pub fn new(
        thread_count: usize,
        supervisor: &Supervisor,
        stats: &Arc<Stats>,
    ) -> crate::Result<(Self, flume::Sender<WorkerMessage>)> {
        let (inner, sender) = WorkerPoolInner::new(thread_count, supervisor, stats)?;
        Ok((Self(Arc::new(inner)), sender))
    }
}

impl std::ops::Deref for WorkerPool {
    type Target = WorkerPoolInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

struct WorkerState {
    worker_id: usize,
    supervisor: Supervisor,
    rx: flume::Receiver<WorkerMessage>,
    sender: flume::Sender<WorkerMessage>,
    stats: Arc<Stats>,
    lock: Arc<Mutex<()>>,
}

// TODO: 3.0.0 replace flume with simple Mutex Queue

fn worker_tick(ctx: &WorkerState) -> crate::Result<bool> {
    // NOTE: We need to lock to get serializable guarantees when looking at flushes of the same keyspace (FIFO)
    let lock = ctx.lock.lock().expect("lock is poisoned");

    let Ok(item) = ctx.rx.recv() else {
        return Ok(true);
    };

    // TODO: 3.0.0 handle stop signal

    log::trace!("Worker #{} got message: {item:?}", ctx.worker_id);

    match item {
        WorkerMessage::Flush => {
            let task = ctx.supervisor.flush_manager.dequeue();

            // IMPORTANT: Lock the keyspace as long as we're doing the flush
            // to prevent a race condition that could install the tables out of order
            let _flush_guard = task.keyspace.acquire_flush_lock();

            drop(lock);

            log::trace!("Performing flush for keyspace {:?}", task.keyspace.name);

            flush_memtable(
                &task,
                &ctx.supervisor.write_buffer_size,
                &ctx.supervisor.snapshot_tracker,
                &ctx.stats,
            )?;

            // TODO: should be thread_pool_size
            for _ in 0..4 {
                ctx.supervisor
                    .compaction_manager
                    .notify(task.keyspace.clone());
            }

            ctx.sender
                .try_send(WorkerMessage::Compact)
                .expect("should work");

            ctx.supervisor
                .journal_manager
                .write()
                .expect("lock is poisoned")
                .maintenance()?;
        }
        WorkerMessage::Compact => {
            drop(lock);

            run_compaction(
                &ctx.supervisor.compaction_manager,
                &ctx.supervisor.snapshot_tracker,
                &ctx.stats,
            )?;
        }
    }

    Ok(false)
}
