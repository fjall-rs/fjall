use fjall::{Database, DurabilityLevel::SyncAll, KeyspaceCreateOptions};
use std::path::Path;
use test_log::test;

fn open(folder: &Path) -> fjall::Result<Database> {
    Database::builder(folder)
        .worker_threads_unchecked(0)
        .disable_write_stalls(true)
        .open()
}

fn table_file_count(folder: &Path) -> std::io::Result<usize> {
    Ok(std::fs::read_dir(folder.join("tables"))?.count())
}

#[test]
fn keyspace_delete_recover() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    {
        let db = open(folder.path())?;
        let keep = db.keyspace("keep", KeyspaceCreateOptions::default)?;
        let a = db.keyspace("a", KeyspaceCreateOptions::default)?;

        keep.insert("k", "keep", SyncAll)?;
        keep.flush(true)?;

        for round in 0..2u8 {
            a.insert([round], "flushed", SyncAll)?;
            a.flush(true)?;
        }
        a.insert("unflushed", "a", SyncAll)?;

        assert_eq!(3, table_file_count(folder.path())?);

        db.delete_keyspace(a)?;
        assert!(!db.keyspace_exists("a"));

        // NOTE: The last handle is gone, so the deleted keyspace's tables are removed
        assert_eq!(1, table_file_count(folder.path())?);
    }

    for _ in 0..2 {
        let db = open(folder.path())?;

        assert!(!db.keyspace_exists("a"));
        assert_eq!(
            vec!["keep"],
            db.list_keyspace_names()
                .iter()
                .map(|n| &**n)
                .collect::<Vec<_>>()
        );

        let keep = db.keyspace("keep", KeyspaceCreateOptions::default)?;
        assert_eq!(Some(b"keep".as_slice()), keep.get("k")?.as_deref());
    }

    Ok(())
}

/// A keyspace created with the name of a deleted keyspace starts empty,
/// the deleted keyspace's WAL items must not end up in it
#[test]
fn keyspace_delete_recreate_same_name() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    {
        let db = open(folder.path())?;
        let a = db.keyspace("a", KeyspaceCreateOptions::default)?;
        a.insert("old", "1", SyncAll)?;
        db.delete_keyspace(a)?;

        let a = db.keyspace("a", KeyspaceCreateOptions::default)?;
        assert!(a.is_empty()?);
        a.insert("new", "2", SyncAll)?;
    }

    for _ in 0..2 {
        let db = open(folder.path())?;
        let a = db.keyspace("a", KeyspaceCreateOptions::default)?;
        assert!(a.get("old")?.is_none());
        assert_eq!(Some(b"2".as_slice()), a.get("new")?.as_deref());
        assert_eq!(1, a.len()?);
    }

    Ok(())
}

/// Regression test for https://github.com/fjall-rs/fjall/issues/328, after a manifest rotation:
/// the snapshot doesn't contain the deleted keyspace anymore, so its ID must not be reused
/// (otherwise its WAL items are replayed into the new keyspace)
#[test]
fn keyspace_delete_id_not_reused_after_rotation() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    {
        let db = open(folder.path())?;
        let _keep = db.keyspace("keep", KeyspaceCreateOptions::default)?;
        let a = db.keyspace("a", KeyspaceCreateOptions::default)?;
        a.insert("stale", "written to a", SyncAll)?;
        db.delete_keyspace(a)?;

        db.rotate_manifest()?;
    }

    {
        let db = open(folder.path())?;
        let b = db.keyspace("b", KeyspaceCreateOptions::default)?;
        assert!(b.is_empty()?, "keyspace b should be empty");
    }

    {
        let db = open(folder.path())?;
        let b = db.keyspace("b", KeyspaceCreateOptions::default)?;
        assert!(b.is_empty()?, "keyspace b should still be empty");
    }

    Ok(())
}

#[test]
fn keyspace_delete_handle_is_unusable() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    let db = open(folder.path())?;
    let a = db.keyspace("a", KeyspaceCreateOptions::default)?;
    a.insert("a", "a", SyncAll)?;

    db.delete_keyspace(a.clone())?;

    assert!(matches!(
        a.insert("b", "b", SyncAll),
        Err(fjall::Error::KeyspaceDeleted),
    ));
    assert!(matches!(a.flush(true), Err(fjall::Error::KeyspaceDeleted)));
    assert!(matches!(a.compact(), Err(fjall::Error::KeyspaceDeleted)));
    assert!(matches!(
        a.major_compact(),
        Err(fjall::Error::KeyspaceDeleted)
    ));
    assert!(matches!(a.clear(), Err(fjall::Error::KeyspaceDeleted)));
    assert!(matches!(
        a.drop_range::<&str, _>(..),
        Err(fjall::Error::KeyspaceDeleted),
    ));
    assert!(matches!(
        a.start_ingestion(),
        Err(fjall::Error::KeyspaceDeleted),
    ));
    assert!(matches!(
        db.delete_keyspace(a),
        Err(fjall::Error::KeyspaceDeleted),
    ));

    Ok(())
}

fn blob_file_count(folder: &Path) -> std::io::Result<usize> {
    Ok(std::fs::read_dir(folder.join("blobs"))?.count())
}

/// A deleted keyspace's tables and blob files stay on disk while a handle to it is still in
/// scope, and are removed from disk once the last handle goes out of scope
#[test]
fn keyspace_delete_removes_files_when_out_of_scope() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    let db = open(folder.path())?;

    let keep = db.keyspace("keep", || {
        KeyspaceCreateOptions::default().with_kv_separation(Some(
            fjall::KvSeparationOptions::default().separation_threshold(1),
        ))
    })?;
    keep.insert("k", "keep", SyncAll)?;
    keep.flush(true)?;

    let a = db.keyspace("a", || {
        KeyspaceCreateOptions::default().with_kv_separation(Some(
            fjall::KvSeparationOptions::default().separation_threshold(1),
        ))
    })?;

    // NOTE: No compaction, so all of a's files belong to its latest version
    for round in 0..3u8 {
        a.insert([round], "value", SyncAll)?;
        a.flush(true)?;
    }

    // NOTE: keep: 1 table + 1 blob file, a: 3 tables + 3 blob files
    let (tables, blob_files) = (
        table_file_count(folder.path())?,
        blob_file_count(folder.path())?,
    );
    assert_eq!(4, tables);
    assert_eq!(4, blob_files);

    let held = a.clone();
    db.delete_keyspace(a)?;

    // NOTE: Still in scope, so nothing may be removed yet
    assert_eq!(tables, table_file_count(folder.path())?);
    assert_eq!(blob_files, blob_file_count(folder.path())?);

    drop(held);

    // NOTE: Only keep's files are left
    assert_eq!(1, table_file_count(folder.path())?);
    assert_eq!(1, blob_file_count(folder.path())?);

    assert_eq!(Some(b"keep".as_slice()), keep.get("k")?.as_deref());

    drop(keep);
    drop(db);

    let db = open(folder.path())?;
    let keep = db.keyspace("keep", KeyspaceCreateOptions::default)?;
    assert_eq!(Some(b"keep".as_slice()), keep.get("k")?.as_deref());
    assert_eq!(1, table_file_count(folder.path())?);
    assert_eq!(1, blob_file_count(folder.path())?);

    Ok(())
}
