// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use std::path::{Path, PathBuf};

pub fn absolute_path(path: &Path) -> PathBuf {
    #[expect(clippy::expect_used, reason = "nothing we can do")]
    std::path::absolute(path).expect("should be absolute path")
}
