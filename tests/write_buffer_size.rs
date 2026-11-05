use fjall::{Database, DurabilityLevel::Buffer, KeyspaceCreateOptions, KvSeparationOptions};
use test_log::test;

#[test]
fn write_buffer_size_after_insert() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    let db = Database::builder(&folder)
        .worker_threads_unchecked(0)
        .open()?;

    let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
    assert_eq!(0, db.write_buffer_size());

    tree.insert("asd", "def", Buffer)?;

    let write_buffer_size_after = db.write_buffer_size();
    assert!(write_buffer_size_after > 0);

    let mut batch = db.batch();
    batch.insert(&tree, "dsa", "qwe");
    batch.commit()?;

    let write_buffer_size_after_batch = db.write_buffer_size();
    assert!(write_buffer_size_after_batch > write_buffer_size_after);

    tree.flush(true)?;
    assert_eq!(0, db.write_buffer_size());

    Ok(())
}

#[test]
fn write_buffer_size_blob() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    let db = Database::builder(&folder)
        .worker_threads_unchecked(0)
        .open()?;

    let tree = db.keyspace("default", || {
        KeyspaceCreateOptions::default().with_kv_separation(Some(KvSeparationOptions::default()))
    })?;
    assert_eq!(0, db.write_buffer_size());

    tree.insert("asd", "def", Buffer)?;

    let write_buffer_size_after = db.write_buffer_size();
    assert!(write_buffer_size_after > 0);

    let mut batch = db.batch();
    batch.insert(&tree, "dsa", "qwe");
    batch.commit()?;

    let write_buffer_size_after_batch = db.write_buffer_size();
    assert!(write_buffer_size_after_batch > write_buffer_size_after);

    tree.flush(true)?;
    assert_eq!(0, db.write_buffer_size());

    Ok(())
}

#[test]
fn write_buffer_size_after_reopen_of_flushed_db() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    {
        let db = Database::builder(&folder)
            .worker_threads_unchecked(0)
            .open()?;

        let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;

        for x in 0u64..1_000 {
            tree.insert(x.to_be_bytes(), "a".repeat(1_000), Buffer)?;
        }

        // Flushed, but still in the active journal
        tree.flush(true)?;
        assert_eq!(0, db.write_buffer_size());
    }

    {
        // Items already in a table must not be replayed back into memory
        let db = Database::builder(&folder).open()?;
        let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
        assert_eq!(0, db.write_buffer_size());
        assert_eq!(1_000, tree.len()?);
    }

    Ok(())
}
