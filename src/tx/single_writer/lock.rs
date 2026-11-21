// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use std::sync::{Arc, Condvar, Mutex, PoisonError};

/// Allows one write transaction at a time.
///
/// Unlike a `MutexGuard`, the guard owns its lock (no lifetime), so a write transaction
/// does not borrow the database.
///
/// NOTE: The inner mutex is only held for a moment and nothing in it can panic,
/// so it can't be poisoned; a panicking transaction just drops its guard.
#[derive(Default)]
pub struct SingleWriterLock {
    locked: Mutex<bool>,
    unlocked: Condvar,
}

impl SingleWriterLock {
    /// Blocks until no other write transaction holds the lock.
    pub fn lock(self: &Arc<Self>) -> SingleWriterGuard {
        let locked = self.locked.lock().unwrap_or_else(PoisonError::into_inner);

        let mut locked = self
            .unlocked
            .wait_while(locked, |locked| *locked)
            .unwrap_or_else(PoisonError::into_inner);

        *locked = true;

        SingleWriterGuard(self.clone())
    }
}

/// Releases the [`SingleWriterLock`] when dropped
pub struct SingleWriterGuard(Arc<SingleWriterLock>);

impl Drop for SingleWriterGuard {
    fn drop(&mut self) {
        *self.0.locked.lock().unwrap_or_else(PoisonError::into_inner) = false;
        self.0.unlocked.notify_one();
    }
}
