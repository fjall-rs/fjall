// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

const STEP_SIZE: usize = 10_000;
const THRESHOLD: usize = 20;

#[allow(clippy::module_name_repetitions)]
pub fn perform_write_stall(l0_runs: usize) {
    if let THRESHOLD..30 = l0_runs {
        let d = l0_runs - THRESHOLD;

        for _ in 0..(d * STEP_SIZE) {
            std::hint::black_box(());
        }
    }
}
