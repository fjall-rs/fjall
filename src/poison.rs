// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use std::sync::{atomic::AtomicBool, Arc};

#[derive(Clone, Debug, Default)]
pub struct PoisonSignal(Arc<AtomicBool>);

impl PoisonSignal {
    pub fn is_poisoned(&self) -> bool {
        self.0.load(std::sync::atomic::Ordering::Acquire)
    }

    pub fn poison(&self) {
        self.0.store(true, std::sync::atomic::Ordering::Release);
    }
}

/// RAII guard to catch panics in background workers and poison a database
#[derive(Clone)]
pub struct PoisonDart(PoisonSignal);

impl PoisonDart {
    pub fn new(signal: PoisonSignal) -> Self {
        Self(signal)
    }

    pub fn poison(&self) {
        self.0.poison();
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
