//! Contains compaction strategies

pub(crate) mod major;
pub(crate) mod tiered;
pub(crate) mod worker;

use crate::level::Levels;

/// Configuration for compaction
#[derive(Debug, Eq, PartialEq)]
pub struct Options {
    /// Segments to compact
    pub segment_ids: Vec<String>,

    /// Level to put the created segments into
    pub dest_level: u8,

    /// Segment target size
    ///
    /// If a segment compaction reaches the level, a new segment is started.
    /// This results in a sorted "run" of segments
    pub target_size: u64,
}

/// Describes what to do (compact or not)
#[derive(Debug, Eq, PartialEq)]
pub enum Choice {
    /// Just do nothing
    DoNothing,

    /// Compacts some segments into a new level
    DoCompact(Options),
}

/// Trait for a compaction strategy
///
/// The strategy receives the levels of the LSM-tree as argument
/// and emits a choice on what to do
pub trait CompactionStrategy {
    /// Decides on what to do based on the current state of the LSM-tree's levels
    fn choose(&self, _: &Levels) -> Choice;
}

pub use tiered::Strategy as SizeTiered;
