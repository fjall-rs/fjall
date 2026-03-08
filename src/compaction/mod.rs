// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

pub(crate) mod worker;

pub use lsm_tree::compaction::{Fifo, Leveled, Levelled};

/// Compaction filter utilities
pub mod filter {
    pub use lsm_tree::compaction::filter::{
        CompactionFilter, Context, Factory, ItemAccessor, Verdict,
    };

    /// Alias for compaction filter return type
    pub type CompactionFilterResult = lsm_tree::Result<Verdict>;
}
