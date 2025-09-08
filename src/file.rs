// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use std::path::Path;

pub const MAGIC_BYTES: &[u8] = &[b'F', b'J', b'L', 2];

pub const JOURNALS_FOLDER: &str = "journals";
pub const KEYSPACES_FOLDER: &str = "keyspaces";

pub const LOCK_FILE: &str = "lock";
pub const FJALL_MARKER: &str = "version";
pub const KEYSPACE_DELETED_MARKER: &str = ".deleted"; // TODO: remove
pub const KEYSPACE_CONFIG_FILE: &str = "config"; // TODO: remove

pub const LSM_MANIFEST_FILE: &str = "manifest";

#[cfg(not(target_os = "windows"))]
pub fn fsync_directory<P: AsRef<Path>>(path: P) -> std::io::Result<()> {
    let path = path.as_ref();

    let file = std::fs::File::open(path).inspect_err(|e| {
        log::error!("Failed to open directory at {}: {e:?}", path.display());
    })?;

    debug_assert!(file.metadata()?.is_dir());

    file.sync_all().inspect_err(|e| {
        log::error!("Failed to fsync directory at {}: {e:?}", path.display());
    })
}

#[cfg(target_os = "windows")]
pub fn fsync_directory<P: AsRef<Path>>(_path: P) -> std::io::Result<()> {
    // Cannot fsync directory on Windows
    Ok(())
}
