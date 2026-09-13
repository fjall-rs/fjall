mod fifo;

use fifo::Queue;
use std::sync::{
    atomic::{AtomicBool, AtomicU8},
    Arc, RwLock,
};

pub struct WriteRecord {
    state: AtomicU8,
    persist_mode: u8,
    batch: (), // TODO: should be enum { item: WriteItem | batch: Vec<WriteItem> }
}

impl WriteRecord {
    pub fn new() -> Self {
        WriteRecord {
            state: AtomicU8::default(),
            persist_mode: 0,
            batch: (),
        }
    }
}

pub struct Pipeline {
    queue: Queue<Arc<WriteRecord>>,
    master_lease: AtomicBool,
    lock: RwLock<()>,
}

impl Pipeline {
    pub fn new() -> Self {
        Self {
            queue: Queue::with_capacity(1_024),
            master_lease: AtomicBool::new(true),
            lock: RwLock::default(),
        }
    }

    pub fn push(&self, record: Arc<WriteRecord>) {
        let lock = self.lock.read().unwrap();

        while self.queue.try_push(record.clone()).is_none() {}

        let mut is_master = self
            .master_lease
            .compare_exchange(
                true,
                false,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Relaxed,
            )
            .is_ok();

        drop(lock);

        'start: loop {
            if is_master {
                // eprintln!("became leader");

                // Process records
                for _ in 0..1_000 {
                    // TODO: sum up batch sizes up until 1 MB or so...

                    let Some(item) = self.queue.try_pop() else {
                        break;
                    };

                    // TODO: process write batch, e.g. get seqno etc.

                    item.state.store(1, std::sync::atomic::Ordering::Release);
                }

                // TODO: fsync or whatever persist wants to do
                // TODO: also publish to snapshot tracker etc. (basically whatever the original write path is doing)

                {
                    let _lock = self.lock.write().unwrap();

                    if let Some(head) = self.queue.peek() {
                        head.state.store(2, std::sync::atomic::Ordering::Release);
                    } else {
                        self.master_lease
                            .store(true, std::sync::atomic::Ordering::Release);
                    }
                }

                return;
            } else {
                // Spin on highest visible seqno, and sometimes check record state

                // eprintln!("[{:?}] spin", std::time::Instant::now());

                loop {
                    let state = record.state.load(std::sync::atomic::Ordering::Relaxed);

                    match state {
                        1 => return,
                        2 => {
                            is_master = true;
                            continue 'start;
                        }
                        _ => {}
                    }
                }
            }
        }
    }
}
