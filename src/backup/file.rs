// Copyright (c) 2026-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::file::remove_file_if_exists;
use std::{
    fs::File,
    path::{Path, PathBuf},
};

pub(super) const SHARED_FOLDER: &str = "shared";
pub(super) const PRIVATE_FOLDER: &str = "private";

/// The backup folder's format version (layout, file naming), see [`check_or_write_version`]
pub(super) const VERSION_FILE: &str = "version";

/// Hard-links `src` to `dst`, or copies it if hard-linking fails
/// (e.g. `dst` is on another filesystem).
///
/// Linked (or copied) under a temporary name first, then renamed, so a crash never leaves
/// a half-written file under its real name (`.tmp` leftovers are removed on open).
///
/// IMPORTANT: `src` must never be written to again, because a hard link shares its contents.
///
/// IMPORTANT: The caller has to fsync the folder of `dst` afterwards.
pub fn link_or_copy(src: &Path, dst: &Path) -> crate::Result<()> {
    let tmp = tmp_path(dst);

    // NOTE: Leftover of a crashed copy, otherwise the hard link fails
    remove_file_if_exists(&tmp)?;

    if let Err(e) = std::fs::hard_link(src, &tmp) {
        log::debug!(
            "Could not hard link {} ({e}), copying instead",
            src.display(),
        );

        std::fs::copy(src, &tmp)?;
        File::open(&tmp)?.sync_all()?;
    }

    std::fs::rename(&tmp, dst)?;

    Ok(())
}

fn tmp_path(dst: &Path) -> PathBuf {
    let mut name = dst.file_name().unwrap_or_default().to_os_string();
    name.push(".tmp");
    dst.with_file_name(name)
}
