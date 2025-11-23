use fjall::{Database, KeyspaceCreateOptions, Readable};
use test_log::test;

#[test]
fn keyspace_iter_dirty_read() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    let db = Database::builder(&folder).open()?;

    let tree = db.keyspace("default", || {
        KeyspaceCreateOptions::default().with_kv_separation(Default::default())
    })?;

    tree.insert("a#1", "a")?;
    tree.insert("a#2", "b")?;
    tree.insert("a#3", "c")?;

    tree.rotate_memtable_and_wait()?;

    let iter = tree.iter();

    tree.insert("a#4", "d")?;

    assert_eq!(3, iter.count());

    Ok(())
}

#[test]
fn keyspace_snapshot_read() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    let db = Database::builder(&folder).open()?;

    let tree = db.keyspace("default", || {
        KeyspaceCreateOptions::default().with_kv_separation(Default::default())
    })?;
    let tree2 = db.keyspace("default2", || {
        KeyspaceCreateOptions::default().with_kv_separation(Default::default())
    })?;

    tree.insert("a#1", "a")?;
    tree.insert("a#2", "b")?;
    tree.insert("a#3", "c")?;
    tree2.insert("b#1", "5")?;

    let snapshot = db.snapshot();

    tree.rotate_memtable_and_wait()?;

    tree.insert("a#4", "d")?;
    tree2.insert("b#2", "6")?;

    assert_eq!(3, snapshot.iter(&tree).count());
    assert_eq!(3, snapshot.iter(&tree).count());

    Ok(())
}

#[test]
fn keyspace_visible_seqno() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    let db = Database::builder(&folder).open()?;

    let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;

    let initial_seqno = db.snapshot().seqno();

    tree.insert("a#1", "a")?;

    let snapshot = db.snapshot();
    assert_eq!(initial_seqno + 1, snapshot.seqno());

    tree.insert("a#2", "a")?;

    let snapshot = db.snapshot();
    assert_eq!(initial_seqno + 2, snapshot.seqno());

    Ok(())
}

#[test]
fn keyspace_torn_read() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    let db = Database::builder(&folder).open()?;

    let initial_seqno = db.snapshot().seqno();

    // Fake start of a batch
    let batch_seqno = db.supervisor.seqno.next();

    // ...

    let snapshot = db.snapshot();
    assert_eq!(initial_seqno, snapshot.seqno());

    // ...

    // Submitting batch completion
    db.supervisor.snapshot_tracker.publish(batch_seqno);

    let snapshot = db.snapshot();
    assert_eq!(initial_seqno + 1, snapshot.seqno());

    Ok(())
}
