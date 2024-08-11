// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use std::sync::atomic::Ordering::Relaxed;
use std::sync::{atomic::AtomicUsize, OnceLock};

static DROP_COUNTER: OnceLock<AtomicUsize> = OnceLock::new();

pub fn increment_drop_counter() {
    get_drop_counter().fetch_add(1, Relaxed);
}

pub fn decrement_drop_counter() {
    get_drop_counter().fetch_sub(1, Relaxed);
}

pub fn get_drop_counter<'a>() -> &'a AtomicUsize {
    DROP_COUNTER.get_or_init(AtomicUsize::default)
}

#[must_use]
pub fn load_drop_counter() -> usize {
    get_drop_counter().load(Relaxed)
}
