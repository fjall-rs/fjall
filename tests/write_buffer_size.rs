use fjall::{Database, KeyspaceCreateOptions, KvSeparationOptions};
use test_log::test;

/// Wait for the write buffer counter to drain to zero after flush.
/// The counter may update asynchronously, especially on Windows.
/// Panics with diagnostic info if the buffer doesn't drain within 2 seconds.
fn wait_for_write_buffer_drain(db: &Database) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while std::time::Instant::now() < deadline {
        if db.write_buffer_size() == 0 {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    panic!(
        "write buffer did not drain before timeout; current={}",
        db.write_buffer_size()
    );
}

#[test]
fn write_buffer_size_after_insert() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    let db = Database::builder(&folder).open()?;

    let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
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
    wait_for_write_buffer_drain(&db);
    assert_eq!(0, db.write_buffer_size());

    Ok(())
}

#[test]
fn write_buffer_size_blob() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    let db = Database::builder(&folder).open()?;

    let tree = db.keyspace("default", || {
        KeyspaceCreateOptions::default().with_kv_separation(Some(KvSeparationOptions::default()))
    })?;
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
    wait_for_write_buffer_drain(&db);
    assert_eq!(0, db.write_buffer_size());

    Ok(())
}
