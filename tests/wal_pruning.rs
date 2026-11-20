use fjall::{Database, DurabilityLevel::SyncAll, KeyspaceCreateOptions};
use std::path::Path;
use test_log::test;

fn open(folder: &Path) -> fjall::Result<Database> {
    Database::builder(folder)
        .worker_threads_unchecked(0)
        .disable_write_stalls(true)
        .open()
}

/// Number of WAL fragments on disk
fn wal_file_count(folder: &Path) -> std::io::Result<usize> {
    let mut count = 0;

    for dirent in std::fs::read_dir(folder)? {
        if dirent?
            .file_name()
            .to_str()
            .is_some_and(|name| name.ends_with(".hwal"))
        {
            count += 1;
        }
    }

    Ok(count)
}

fn assert_wal_count(db: &Database, folder: &Path, expected: usize) -> std::io::Result<()> {
    assert_eq!(expected, db.wal_count(), "journal count");
    assert_eq!(expected, wal_file_count(folder)?, "WAL files on disk");
    Ok(())
}

/// A sealed fragment is only pruned once every keyspace that has items in it is flushed
#[test]
fn wal_pruned_once_all_keyspaces_flushed() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;
    let db = open(folder.path())?;

    let a = db.keyspace("a", KeyspaceCreateOptions::default)?;
    let b = db.keyspace("b", KeyspaceCreateOptions::default)?;

    a.insert("a", "a", SyncAll)?;
    b.insert("b", "b", SyncAll)?;

    db.rotate_wal()?;
    assert_wal_count(&db, folder.path(), 2)?;

    // NOTE: b's item is still only in a memtable (and the sealed fragment)
    a.flush(true)?;
    assert_wal_count(&db, folder.path(), 2)?;

    b.flush(true)?;
    assert_wal_count(&db, folder.path(), 1)?;

    Ok(())
}

/// Rotating an empty WAL fragment does nothing
#[test]
fn wal_rotate_empty_does_nothing() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;
    let db = open(folder.path())?;

    let a = db.keyspace("a", KeyspaceCreateOptions::default)?;

    db.rotate_wal()?;
    assert_wal_count(&db, folder.path(), 1)?;

    a.insert("a", "a", SyncAll)?;
    db.rotate_wal()?;
    db.rotate_wal()?;
    assert_wal_count(&db, folder.path(), 2)?;

    Ok(())
}

/// Older fragments are pruned first: a fragment that is still needed keeps
/// all newer fragments, even if they are fully flushed
#[test]
fn wal_pruned_oldest_first() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;
    let db = open(folder.path())?;

    let a = db.keyspace("a", KeyspaceCreateOptions::default)?;
    let b = db.keyspace("b", KeyspaceCreateOptions::default)?;

    // 1. fragment #0: a + b
    a.insert("a0", "a", SyncAll)?;
    b.insert("b0", "b", SyncAll)?;
    db.rotate_wal()?;

    // 2. fragment #1: a only
    a.insert("a1", "a", SyncAll)?;
    db.rotate_wal()?;
    assert_wal_count(&db, folder.path(), 3)?;

    // NOTE: #1 is flushed, but #0 still needs b
    a.flush(true)?;
    assert_wal_count(&db, folder.path(), 3)?;

    b.flush(true)?;
    assert_wal_count(&db, folder.path(), 1)?;

    Ok(())
}

/// A deleted keyspace's items are never replayed, so they don't keep fragments around
#[test]
fn wal_pruning_ignores_deleted_keyspace() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;
    let db = open(folder.path())?;

    let a = db.keyspace("a", KeyspaceCreateOptions::default)?;
    let b = db.keyspace("b", KeyspaceCreateOptions::default)?;

    a.insert("a", "a", SyncAll)?;
    b.insert("b", "b", SyncAll)?;
    db.rotate_wal()?;

    db.delete_keyspace(b)?;

    a.flush(true)?;
    assert_wal_count(&db, folder.path(), 1)?;

    Ok(())
}

/// A clear removes the keyspace's items on purpose, so they don't keep fragments around
#[test]
fn wal_pruned_after_clear() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    {
        let db = open(folder.path())?;
        let a = db.keyspace("a", KeyspaceCreateOptions::default)?;

        a.insert("a", "a", SyncAll)?;
        db.rotate_wal()?;
        assert_wal_count(&db, folder.path(), 2)?;

        a.clear()?;
        assert_wal_count(&db, folder.path(), 1)?;
    }

    {
        let db = open(folder.path())?;
        let a = db.keyspace("a", KeyspaceCreateOptions::default)?;
        assert!(a.is_empty()?);
    }

    Ok(())
}

/// An ingestion flushes the memtables first, so the sealed fragment can be pruned
#[test]
fn wal_pruned_after_ingestion() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    {
        let db = open(folder.path())?;
        let a = db.keyspace("a", KeyspaceCreateOptions::default)?;

        a.insert("a", "written", SyncAll)?;
        db.rotate_wal()?;
        assert_wal_count(&db, folder.path(), 2)?;

        let mut ingestion = a.start_ingestion()?;
        ingestion.write("b", "ingested")?;
        ingestion.finish()?;

        assert_wal_count(&db, folder.path(), 1)?;
    }

    {
        let db = open(folder.path())?;
        let a = db.keyspace("a", KeyspaceCreateOptions::default)?;
        assert_eq!(Some(b"written".as_slice()), a.get("a")?.as_deref());
        assert_eq!(Some(b"ingested".as_slice()), a.get("b")?.as_deref());
    }

    Ok(())
}

/// Recovery rebuilds the watermarks of sealed fragments from the replayed items,
/// so a fragment is kept until its replayed items are flushed again
#[test]
fn wal_recovery_restores_watermarks() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    {
        let db = open(folder.path())?;
        let a = db.keyspace("a", KeyspaceCreateOptions::default)?;
        let b = db.keyspace("b", KeyspaceCreateOptions::default)?;

        a.insert("a0", "a", SyncAll)?;
        b.insert("b0", "b", SyncAll)?;
        db.rotate_wal()?;

        a.insert("a1", "a", SyncAll)?;
        a.flush(true)?;

        // NOTE: b0 is only in the sealed fragment
        assert_wal_count(&db, folder.path(), 2)?;
    }

    {
        let db = open(folder.path())?;
        let b = db.keyspace("b", KeyspaceCreateOptions::default)?;

        assert_wal_count(&db, folder.path(), 2)?;
        assert_eq!(Some(b"b".as_slice()), b.get("b0")?.as_deref());

        b.flush(true)?;
        assert_wal_count(&db, folder.path(), 1)?;
    }

    {
        let db = open(folder.path())?;
        let a = db.keyspace("a", KeyspaceCreateOptions::default)?;
        let b = db.keyspace("b", KeyspaceCreateOptions::default)?;

        assert_eq!(2, a.len()?);
        assert_eq!(Some(b"b".as_slice()), b.get("b0")?.as_deref());
        assert_wal_count(&db, folder.path(), 1)?;
    }

    Ok(())
}

/// Recovery prunes sealed fragments whose items are all flushed already
#[test]
fn wal_recovery_prunes_flushed_fragments() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    {
        let db = open(folder.path())?;
        let a = db.keyspace("a", KeyspaceCreateOptions::default)?;

        a.insert("a0", "a", SyncAll)?;
        db.rotate_wal()?;
        a.insert("a1", "a", SyncAll)?;
        db.rotate_wal()?;
        a.insert("a2", "a", SyncAll)?;

        // NOTE: Only seals the memtable, so nothing is flushed (and pruned) yet
        a.rotate_memtable()?;
        assert_wal_count(&db, folder.path(), 3)?;
    }

    {
        let db = open(folder.path())?;
        let a = db.keyspace("a", KeyspaceCreateOptions::default)?;

        // NOTE: Everything was replayed into memtables
        assert_wal_count(&db, folder.path(), 3)?;

        a.flush(true)?;
        assert_wal_count(&db, folder.path(), 1)?;
    }

    {
        let db = open(folder.path())?;
        let a = db.keyspace("a", KeyspaceCreateOptions::default)?;
        assert_eq!(3, a.len()?);
        assert_wal_count(&db, folder.path(), 1)?;
    }

    Ok(())
}

/// Recovery prunes sealed fragments that are not needed anymore right away,
/// e.g. if they only contain items of a deleted keyspace
#[test]
fn wal_recovery_prunes_unneeded_fragments() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    {
        let db = open(folder.path())?;
        let _a = db.keyspace("a", KeyspaceCreateOptions::default)?;
        let b = db.keyspace("b", KeyspaceCreateOptions::default)?;

        b.insert("b", "b", SyncAll)?;
        db.rotate_wal()?;

        // NOTE: Deleting a keyspace does not prune the WAL
        db.delete_keyspace(b)?;
        assert_wal_count(&db, folder.path(), 2)?;
    }

    {
        let db = open(folder.path())?;
        assert_wal_count(&db, folder.path(), 1)?;
        assert!(!db.keyspace_exists("b"));
    }

    Ok(())
}
