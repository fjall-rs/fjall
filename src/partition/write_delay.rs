// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

/// Gets the write delay based on L0 segments.
///
/// The write delay increases linearly as L0 approaches 20 segments.
#[allow(clippy::module_name_repetitions)]
pub fn get_write_delay(l0_segments: usize) -> u64 {
    match l0_segments {
        20 => 10,
        21 => 20,
        22 => 30,
        23 => 40,
        24 => 50,
        25 => 60,
        26 => 70,
        27 => 80,
        28 => 100,
        29 => 200,
        30 => 500,
        31 => 1_000,
        _ => 0,
    }
}
