// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use std::sync::atomic::{AtomicU64, AtomicUsize};

/// Ephemeral, runtime stats
#[derive(Default)]
pub struct Stats {
    /// Active compaction conter
    pub(crate) active_compaction_count: AtomicUsize,

    /// Time spent in compactions (in µs)
    pub(crate) time_compacting: AtomicU64,

    /// Number of completed compactions
    pub(crate) compactions_completed: AtomicUsize,
}

impl Stats {}
