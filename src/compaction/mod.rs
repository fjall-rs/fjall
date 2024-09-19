// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

pub(crate) mod manager;
pub(crate) mod worker;

use std::sync::Arc;

pub use lsm_tree::compaction::{Fifo, Leveled, Levelled, SizeTiered};

/// Compaction strategy
#[derive(Clone)]
#[allow(clippy::module_name_repetitions)]
pub enum Strategy {
    /// Leveled compaction
    Leveled(crate::compaction::Leveled),

    /// Size-tiered compaction
    SizeTiered(crate::compaction::SizeTiered),

    /// FIFO compaction
    Fifo(crate::compaction::Fifo),
}

impl std::fmt::Debug for Strategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::SizeTiered(_) => "SizeTieredStrategy",
                Self::Leveled(_) => "LeveledStrategy",
                Self::Fifo(_) => "FifoStrategy",
            }
        )
    }
}

impl Default for Strategy {
    fn default() -> Self {
        Self::Leveled(crate::compaction::Leveled::default())
    }
}

impl Strategy {
    pub(crate) fn inner(&self) -> Arc<dyn lsm_tree::compaction::CompactionStrategy + Send + Sync> {
        match self {
            Self::Leveled(s) => Arc::new(s.clone()),
            Self::SizeTiered(s) => Arc::new(s.clone()),
            Self::Fifo(s) => Arc::new(s.clone()),
        }
    }
}
