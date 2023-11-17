use crate::{disk_block::DiskBlock, Value};

/// Value blocks are the building blocks of a [`Segment`]. Each block is a sorted list of [`Value`]s,
/// and stored in compressed form on disk.
///
/// The integrity of a block can be checked using the CRC value that is saved in it.
pub type ValueBlock = DiskBlock<Value>;
