use std::sync::{atomic::AtomicBool, Arc};

type PoisonSignal = Arc<AtomicBool>;

/// RAII guard to catch panics in background workers
/// and poison a keyspace
pub struct PoisonDart {
    name: &'static str,
    signal: PoisonSignal,
}

impl PoisonDart {
    pub fn new(name: &'static str, signal: PoisonSignal) -> Self {
        Self { name, signal }
    }

    pub fn poison(&self) {
        self.signal
            .store(true, std::sync::atomic::Ordering::Release);
    }
}

impl Drop for PoisonDart {
    fn drop(&mut self) {
        if std::thread::panicking() {
            log::error!(
                "Poisoning keyspace because of panic in background worker {:?}",
                self.name
            );
            self.poison();
        }
    }
}
