use std::sync::atomic::{AtomicU64, AtomicUsize};

/// Ephemeral, runtime stats
#[derive(Default)]
pub struct Stats {
    /// Active compaction conter
    pub(crate) active_compaction_count: AtomicUsize,

    /// Time spent in compactions (in µs)
    pub(crate) time_compacting: AtomicU64,

    /// Time spent in garbage collection (in µs)
    pub(crate) time_gc: AtomicU64,

    /// Number of created memtable flush tasks
    pub(crate) flushes_enqueued: AtomicUsize,

    /// Number of completed memtable flushes
    pub(crate) flushes_completed: AtomicUsize,

    /// Number of completed compactions
    pub(crate) compactions_completed: AtomicUsize,
}

impl Stats {
    pub fn outstanding_flushes(&self) -> usize {
        self.flushes_enqueued
            .load(std::sync::atomic::Ordering::Relaxed)
            - self
                .flushes_completed
                .load(std::sync::atomic::Ordering::Relaxed)
    }
}
