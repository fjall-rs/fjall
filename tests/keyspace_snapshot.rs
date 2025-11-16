use fjall::{Database, KeyspaceCreateOptions, Readable};
use test_log::test;

#[test]
fn keyspace_iter_dirty_read() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    let db = Database::builder(&folder).open()?;

    let tree = db.keyspace(
        "default",
        KeyspaceCreateOptions::default().with_kv_separation(Default::default()),
    )?;

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

    let tree = db.keyspace(
        "default",
        KeyspaceCreateOptions::default().with_kv_separation(Default::default()),
    )?;
    let tree2 = db.keyspace(
        "default2",
        KeyspaceCreateOptions::default().with_kv_separation(Default::default()),
    )?;

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
