// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

pub(crate) mod manager;
pub(crate) mod worker;

pub use lsm_tree::compaction::{
    CompactionStrategy as Strategy, Fifo, Leveled, Levelled, SizeTiered,
};
