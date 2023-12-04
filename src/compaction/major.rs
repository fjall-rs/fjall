use super::{Choice, CompactionInput, CompactionStrategy};
use crate::levels::Levels;
use std::sync::Arc;

/// Major compaction
///
/// Compacts all segments into the last level
pub struct Strategy {
    target_size: u64,
}

impl Strategy {
    /// Configures a new `SizeTiered` compaction strategy
    ///
    /// # Panics
    ///
    /// Panics, if `target_size` is below 1024 bytes
    #[must_use]
    pub fn new(target_size: u64) -> Arc<Self> {
        assert!(target_size >= 1024);

        Arc::new(Self { target_size })
    }
}

impl Default for Strategy {
    fn default() -> Self {
        Self {
            target_size: u64::MAX,
        }
    }
}

impl CompactionStrategy for Strategy {
    fn choose(&self, levels: &Levels) -> Choice {
        let segments = levels.get_segments();
        let segment_ids = segments.values().map(|s| s.metadata.id.clone()).collect();

        Choice::DoCompact(CompactionInput {
            segment_ids,
            dest_level: levels.depth() as u8 - 1,
            target_size: self.target_size,
        })
    }
}
