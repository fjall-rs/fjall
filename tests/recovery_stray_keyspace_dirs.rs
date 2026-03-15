use fjall::Database;
use test_log::test;

/// Recovery must skip directories with non-numeric names in the keyspaces folder
/// instead of panicking.
#[test]
fn recovery_skips_non_numeric_keyspace_dir() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    {
        let _db = Database::builder(&folder).open()?;
    }

    // Create a stray directory with a non-numeric name inside keyspaces/
    std::fs::create_dir_all(folder.path().join("keyspaces").join("not-a-number"))?;

    {
        // Must not panic — should log a warning and skip
        let _db = Database::builder(&folder).open()?;
    }

    Ok(())
}

/// Recovery must skip directories with non-UTF-8 names in the keyspaces folder.
/// Only runs on Linux — macOS (HFS+/APFS) rejects invalid byte sequences in filenames.
#[cfg(target_os = "linux")]
#[test]
fn recovery_skips_non_utf8_keyspace_dir() -> fjall::Result<()> {
    use std::os::unix::ffi::OsStrExt;

    let folder = tempfile::tempdir()?;

    {
        let _db = Database::builder(&folder).open()?;
    }

    // Create a directory with invalid UTF-8 bytes (only works on Linux/ext4)
    let bad_name = std::ffi::OsStr::from_bytes(&[0xff, 0xfe]);
    std::fs::create_dir_all(folder.path().join("keyspaces").join(bad_name))?;

    {
        // Must not panic — should log a warning and skip
        let _db = Database::builder(&folder).open()?;
    }

    Ok(())
}
