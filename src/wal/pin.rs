// Copyright (c) 2026-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

/// Keeps the WAL from being pruned for as long as it is alive,
/// e.g. while a backup copies the WAL fragments
///
/// Dropping it unpins the WAL again, without needing the ledger lock;
/// the next prune then catches up.
#[must_use]
pub struct WalPin(pub(super) Arc<AtomicBool>);

impl Drop for WalPin {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}
