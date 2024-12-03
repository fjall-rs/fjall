// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

/// Gets the write delay based on L0 segments.
///
/// The write delay increases linearly as L0 approaches 20 segments.
#[allow(clippy::module_name_repetitions)]
pub fn get_write_delay(l0_segments: usize) -> u64 {
    match l0_segments {
        20 => 1,
        21 => 2,
        22 => 4,
        23 => 8,
        24 => 16,
        25 => 32,
        26 => 64,
        27 => 128,
        28 => 256,
        29 => 512,
        _ => 0,
    }
}
