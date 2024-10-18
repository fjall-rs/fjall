// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

const VALID_CHARACTERS: &str =
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-.#$";

/// Partition names can be up to 255 characters long, can not be empty and
/// can only contain alphanumerics, underscore (`_`), dash (`-`), dot (`.`), hash tag (`#`) and dollar (`$`).
#[allow(clippy::module_name_repetitions)]
pub fn is_valid_partition_name(s: &str) -> bool {
    if s.is_empty() {
        return false;
    }

    if u8::try_from(s.len()).is_err() {
        return false;
    }

    s.chars().all(|c| VALID_CHARACTERS.contains(c))
}
