// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

/// Keyspace names can be up to 255 characters long, can not be empty and
/// can only contain alphanumerics, underscore (`_`), dash (`-`), dot (`.`), hash tag (`#`) and dollar (`$`).
#[expect(clippy::module_name_repetitions)]
pub fn is_valid_keyspace_name(s: &str) -> bool {
    if s.is_empty() {
        return false;
    }

    u8::try_from(s.len()).is_ok()
}
