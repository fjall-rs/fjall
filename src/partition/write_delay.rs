// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

/// Gets the write delay based on L0 segments.
///
/// The write delay increases exponentially as L0 approaches 20 segments.
#[allow(clippy::module_name_repetitions)]
pub fn get_write_delay(l0_segments: usize) -> u64 {
    match l0_segments {
        10 => 10,
        11 => 20,
        12 => 30,
        13 => 40,
        14 => 50,
        15 => 60,
        16 => 70,
        17 => 80,
        18 => 100,
        19 => 200,
        20 => 500,
        _ => 0,
    }
}
