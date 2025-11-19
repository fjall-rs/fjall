use std::sync::{atomic::AtomicBool, Arc};

type PoisonSignal = Arc<AtomicBool>;

/// RAII guard to catch panics in background workers
/// and poison a database
#[derive(Clone)]
pub struct PoisonDart {
    signal: PoisonSignal,
}

impl PoisonDart {
    pub fn new(signal: PoisonSignal) -> Self {
        Self { signal }
    }

    pub fn poison(&self) {
        self.signal
            .store(true, std::sync::atomic::Ordering::Release);
    }
}

impl Drop for PoisonDart {
    fn drop(&mut self) {
        if std::thread::panicking() {
            log::error!("Poisoning database because of panic in background worker");
            self.poison();
        }
    }
}
