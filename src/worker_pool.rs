use crate::{
    compaction::worker::run as run_compaction, flush::worker::run as run_flush,
    poison_dart::PoisonDart, stats::Stats, supervisor::Supervisor, Keyspace,
};
use std::{
    borrow::Cow,
    sync::{atomic::AtomicUsize, Arc},
    thread::JoinHandle,
};

pub enum WorkerMessage {
    Flush,
    Compact(Keyspace),
    Close,
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
            }
        )
    }
}

type WorkerHandle = JoinHandle<Result<(), crate::Error>>;

// TODO: 3.0.0 probably no Inner needed, just give messager to keyspaces
// TODO: let DB own worker pool so it can consume it on drop

pub struct WorkerPool {
    thread_handles: Vec<WorkerHandle>,
    pub(crate) rx: flume::Receiver<WorkerMessage>,
}

impl WorkerPool {
    pub fn new(
        pool_size: usize,
        supervisor: &Supervisor,
        stats: &Arc<Stats>,
        thread_counter: &Arc<AtomicUsize>,
        poison_dart: &PoisonDart,
    ) -> crate::Result<(Self, flume::Sender<WorkerMessage>)> {
        use std::sync::atomic::Ordering::Relaxed;

        let (message_queue_sender, rx) = flume::bounded(1_000);

        thread_counter.fetch_add(pool_size, Relaxed);

        let thread_handles = (0..pool_size)
            .map(|i| {
                std::thread::Builder::new()
                    .name("fjall:worker".to_string())
                    .spawn({
                        log::debug!("Starting fjall worker thread #{i}");

                        let worker_state = WorkerState {
                            pool_size,
                            worker_id: i,
                            rx: rx.clone(),
                            supervisor: supervisor.clone(),
                            stats: stats.clone(),
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

        Ok((Self { thread_handles, rx }, message_queue_sender))
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
        WorkerMessage::Flush => {
            let Some(task) = ctx.supervisor.flush_manager.dequeue() else {
                return Ok(false);
            };

            log::trace!("Performing flush for keyspace {:?}", task.keyspace.name);

            run_flush(
                &task,
                &ctx.supervisor.write_buffer_size,
                &ctx.supervisor.snapshot_tracker,
                &ctx.stats,
            )?;

            // TODO: 3.0.0 should be thread_pool_size
            for _ in 0..4 {
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
