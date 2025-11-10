use crate::{
    compaction::worker::run as run_compaction,
    flush::worker::run as flush_memtable,
    poison_dart::{self, PoisonDart},
    stats::Stats,
    supervisor::Supervisor,
};
use std::{
    sync::{atomic::AtomicUsize, Arc, Mutex},
    thread::JoinHandle,
};

pub enum WorkerMessage {
    Flush,
    Compact,
    Close,
}

impl std::fmt::Debug for WorkerMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::Flush => "WorkerMessage:Flush",
                Self::Compact => "WorkerMessage:Compact",
                Self::Close => "WorkerMessage:Close",
            }
        )
    }
}

type WorkerHandle = JoinHandle<Result<(), crate::Error>>;

// TODO: 3.0.0 probably no Inner needed, just give messager to keyspaces
// TODO: let DB own worker pool so it can consume it on drop

pub struct WorkerPoolInner {
    thread_handles: Vec<WorkerHandle>,
    lock: Arc<Mutex<()>>,
    pub(crate) rx: flume::Receiver<WorkerMessage>,
}

impl WorkerPoolInner {
    pub fn new(
        pool_size: usize,
        supervisor: &Supervisor,
        stats: &Arc<Stats>,
        thread_counter: &Arc<AtomicUsize>,
        poison_dart: &PoisonDart,
    ) -> crate::Result<(Self, flume::Sender<WorkerMessage>)> {
        use std::sync::atomic::Ordering::Relaxed;

        let (message_queue_sender, rx) = flume::bounded(10_000);
        let lock = Arc::new(Mutex::default());

        thread_counter.fetch_add(pool_size, Relaxed);

        let thread_handles = (0..pool_size)
            .map(|i| {
                std::thread::Builder::new()
                    .name("fjall:worker".to_string())
                    .spawn({
                        log::debug!("Starting fjall worker thread #{i}");

                        let worker_state = WorkerState {
                            worker_id: i,
                            rx: rx.clone(),
                            supervisor: supervisor.clone(),
                            stats: stats.clone(),
                            lock: lock.clone(),
                            sender: message_queue_sender.clone(),
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

        Ok((
            Self {
                thread_handles,
                lock,
                rx,
            },
            message_queue_sender,
        ))
    }
}

#[derive(Clone)]
pub struct WorkerPool(Arc<WorkerPoolInner>);

impl WorkerPool {
    pub fn new(
        pool_size: usize,
        supervisor: &Supervisor,
        stats: &Arc<Stats>,
        thread_counter: &Arc<AtomicUsize>,
        poison_dart: &PoisonDart,
    ) -> crate::Result<(Self, flume::Sender<WorkerMessage>)> {
        let (inner, sender) =
            WorkerPoolInner::new(pool_size, supervisor, stats, thread_counter, poison_dart)?;

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

    log::trace!("Worker #{} got message: {item:?}", ctx.worker_id);

    match item {
        WorkerMessage::Close => {
            return Ok(true);
        }
        WorkerMessage::Flush => {
            let Some(task) = ctx.supervisor.flush_manager.dequeue() else {
                return Ok(false);
            };

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
                    .push(task.keyspace.clone());

                ctx.sender.try_send(WorkerMessage::Compact).ok();
            }

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
