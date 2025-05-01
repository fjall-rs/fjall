use crate::poison_dart::PoisonDart;
use lsm_tree::stop_signal::StopSignal;
use std::sync::{atomic::AtomicUsize, Arc};

pub trait Activity {
    /// Gets the name of the activity.
    fn name(&self) -> &'static str;

    /// Runs the activity once.
    fn run(&mut self) -> crate::Result<()>;
}

pub struct BackgroundWorker<A: Activity> {
    activity: A,
    poison_dart: PoisonDart,
    thread_counter: Arc<AtomicUsize>,
    stop_signal: StopSignal,
}

impl<A: Activity> BackgroundWorker<A> {
    pub fn new(
        activity: A,
        poison_dart: PoisonDart,
        thread_counter: Arc<AtomicUsize>,
        stop_signal: StopSignal,
    ) -> Self {
        Self {
            activity,
            poison_dart,
            thread_counter,
            stop_signal,
        }
    }

    /// Starts the background activity.
    ///
    /// Does not start a thread; this function is blocking.
    pub fn start(mut self) {
        log::debug!("Starting background worker {:?}", self.activity.name());

        self.thread_counter
            .fetch_add(1, std::sync::atomic::Ordering::Release);

        loop {
            if self.stop_signal.is_stopped() {
                log::trace!(
                    "Background worker {:?} exiting because keyspace is dropping",
                    self.activity.name(),
                );
                return;
            }

            if let Err(e) = self.activity.run() {
                eprintln!("Background worker {:?} failed: {e:?}", self.activity.name());
                self.poison_dart.poison();
                return;
            }
        }
    }
}

impl<A: Activity> Drop for BackgroundWorker<A> {
    fn drop(&mut self) {
        self.thread_counter
            .fetch_sub(1, std::sync::atomic::Ordering::Release);
    }
}
