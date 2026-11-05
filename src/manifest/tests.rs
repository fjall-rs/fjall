use crate::{Database, DurabilityLevel::SyncAll, KeyspaceCreateOptions};
use std::path::Path;
use test_log::test;

/// Writes a database with a keyspace and some flushed + unflushed data
fn write_db(folder: &Path) -> crate::Result<()> {
    let db = Database::builder(folder).open()?;
    let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;

    tree.insert("a", "flushed", SyncAll)?;
    tree.flush(true)?;
    tree.insert("b", "unflushed", SyncAll)?;

    Ok(())
}

fn assert_db(folder: &Path) -> crate::Result<()> {
    let db = Database::builder(folder).open()?;
    let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;

    assert_eq!(Some(b"flushed".as_slice()), tree.get("a")?.as_deref());
    assert_eq!(Some(b"unflushed".as_slice()), tree.get("b")?.as_deref());
    assert_eq!(2, tree.len()?);

    Ok(())
}

fn set_current(folder: &Path, id: u64) -> std::io::Result<()> {
    std::fs::write(folder.join("current"), id.to_be_bytes())
}

/// A manifest rotation writes a full snapshot into the new fragment, then updates `current`,
/// so older fragments are stale and must be ignored (and removed)
#[test]
fn manifest_recovery_ignores_older_fragments() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    write_db(folder.path())?;

    // NOTE: hwal fragments don't contain their own ID, so a copy of v0 is a valid v1 (= the snapshot)
    std::fs::copy(folder.path().join("v0"), folder.path().join("v1"))?;
    std::fs::write(folder.path().join("v0"), b"garbage, must not be read")?;
    set_current(folder.path(), 1)?;

    assert_db(folder.path())?;

    assert!(!folder.path().join("v0").try_exists()?, "v0 should be removed");
    assert!(folder.path().join("v1").try_exists()?, "v1 should be kept");

    assert_db(folder.path())?;

    Ok(())
}

/// A rotation that crashed before updating `current` leaves a newer fragment behind,
/// which was never committed and must be ignored (and removed)
#[test]
fn manifest_recovery_ignores_uncommitted_newer_fragment() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    write_db(folder.path())?;

    std::fs::write(folder.path().join("v1"), b"garbage, must not be read")?;

    assert_db(folder.path())?;

    assert!(folder.path().join("v0").try_exists()?, "v0 should be kept");
    assert!(!folder.path().join("v1").try_exists()?, "v1 should be removed");

    Ok(())
}

/// If `current` names a fragment that does not exist, nothing may be removed
#[test]
fn manifest_recovery_missing_current_fragment() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    write_db(folder.path())?;

    set_current(folder.path(), 5)?;

    assert!(Database::builder(folder.path()).open().is_err());
    assert!(folder.path().join("v0").try_exists()?, "v0 should be kept");

    // NOTE: Fixing the marker makes the database usable again
    set_current(folder.path(), 0)?;
    assert_db(folder.path())?;

    Ok(())
}

/// A corrupted `current` marker is an error, nothing may be removed
#[test]
fn manifest_recovery_corrupted_current_marker() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    write_db(folder.path())?;

    std::fs::write(folder.path().join("current"), b"abc")?;

    assert!(Database::builder(folder.path()).open().is_err());
    assert!(folder.path().join("v0").try_exists()?, "v0 should be kept");

    Ok(())
}

/// Other files starting with `v` are not manifest fragments, so they must not be touched
#[test]
fn manifest_recovery_keeps_unrelated_files() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    write_db(folder.path())?;

    for name in ["v0.bak", "vfoo", "v", "v1x"] {
        std::fs::write(folder.path().join(name), b"not a manifest fragment")?;
    }

    assert_db(folder.path())?;

    for name in ["v0.bak", "vfoo", "v", "v1x", "version"] {
        assert!(
            folder.path().join(name).try_exists()?,
            "{name} should be kept",
        );
    }

    Ok(())
}
