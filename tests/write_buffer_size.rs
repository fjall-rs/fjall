use fjall::{Database, KeyspaceCreateOptions, KvSeparationOptions};
use test_log::test;

#[test]
fn write_buffer_size_after_insert() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    let db = Database::builder(&folder).open()?;

    let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
    assert_eq!(0, db.write_buffer_size());

    tree.insert("asd", "def")?;

    let write_buffer_size_after = db.write_buffer_size();
    assert!(write_buffer_size_after > 0);

    let mut batch = db.batch();
    batch.insert(&tree, "dsa", "qwe");
    batch.commit()?;

    let write_buffer_size_after_batch = db.write_buffer_size();
    assert!(write_buffer_size_after_batch > write_buffer_size_after);

    tree.rotate_memtable_and_wait()?;
    assert_eq!(0, db.write_buffer_size());

    Ok(())
}

#[test]
fn write_buffer_size_blob() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    let db = Database::builder(&folder).open()?;

    let tree = db.keyspace(
        "default",
        KeyspaceCreateOptions::default().with_kv_separation(KvSeparationOptions::default()),
    )?;
    assert_eq!(0, db.write_buffer_size());

    tree.insert("asd", "def")?;

    let write_buffer_size_after = db.write_buffer_size();
    assert!(write_buffer_size_after > 0);

    let mut batch = db.batch();
    batch.insert(&tree, "dsa", "qwe");
    batch.commit()?;

    let write_buffer_size_after_batch = db.write_buffer_size();
    assert!(write_buffer_size_after_batch > write_buffer_size_after);

    tree.rotate_memtable_and_wait()?;
    assert_eq!(0, db.write_buffer_size());

    Ok(())
}
