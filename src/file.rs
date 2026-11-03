// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use std::{io::Write, path::Path};

pub const TABLES_FOLDER: &str = "tables";
pub const BLOBS_FOLDER: &str = "blobs";
pub const LOCK_FILE: &str = "lock";
pub const VERSION_MARKER: &str = "version";

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

/// Runs an I/O operation, retrying transient Windows errors with backoff.
///
/// External programs (antivirus, indexers, backup agents) may briefly hold files open
/// without sharing flags on Windows, failing renames and deletes that would succeed a
/// moment later; POSIX has no such failure mode, so elsewhere the operation runs once.
fn retry_transient_io<T>(mut op: impl FnMut() -> std::io::Result<T>) -> std::io::Result<T> {
    #[cfg(windows)]
    {
        const MAX_ATTEMPTS: u32 = 10;

        let mut delay = std::time::Duration::from_millis(1);

        for _ in 1..MAX_ATTEMPTS {
            match op() {
                Err(e) if is_transient_windows_error(&e) => {
                    std::thread::sleep(delay);
                    delay *= 2;
                }
                result => return result,
            }
        }
    }

    op()
}

#[cfg(windows)]
fn is_transient_windows_error(e: &std::io::Error) -> bool {
    // ERROR_ACCESS_DENIED, ERROR_SHARING_VIOLATION, ERROR_LOCK_VIOLATION, ERROR_USER_MAPPED_FILE
    matches!(e.raw_os_error(), Some(5 | 32 | 33 | 1224))
}

/// Persists a named temporary file to its final path, replacing any existing file.
fn persist_temp_file(temp_file: tempfile::NamedTempFile, path: &Path) -> std::io::Result<()> {
    let mut temp_file = Some(temp_file);

    retry_transient_io(|| {
        #[expect(clippy::expect_used, reason = "the temp file is put back on failure")]
        temp_file
            .take()
            .expect("temp file should be present")
            .persist(path)
            .map(|_| ())
            .map_err(|e| {
                temp_file = Some(e.file);
                e.error
            })
    })
}

/// Atomically rewrites a file.
pub fn rewrite_atomic(path: &Path, content: &[u8]) -> std::io::Result<()> {
    #[expect(
        clippy::expect_used,
        reason = "every file should have a parent directory"
    )]
    let folder = path.parent().expect("should have a parent");

    let mut temp_file = tempfile::NamedTempFile::new_in(folder)?;
    temp_file.write_all(content)?;
    temp_file.flush()?;
    temp_file.as_file_mut().sync_all()?;
    persist_temp_file(temp_file, path)?;

    // TODO: not sure why it fails on Windows...
    #[cfg(not(target_os = "windows"))]
    {
        let file = std::fs::File::open(path)?;
        file.sync_all()?;

        #[expect(
            clippy::expect_used,
            reason = "files should always have a parent directory"
        )]
        let folder = path.parent().expect("should have parent folder");
        fsync_directory(folder)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn atomic_rewrite() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;

        let path = dir.path().join("test.txt");
        {
            let mut file = std::fs::File::create(&path)?;
            write!(file, "asdasdasdasdasd")?;
        }

        rewrite_atomic(&path, b"newcontent")?;

        let content = std::fs::read_to_string(&path)?;
        assert_eq!("newcontent", content);

        Ok(())
    }
}
