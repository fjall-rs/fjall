use fjall::Readable;

#[test_log::test]
fn fifo_dirty_read() -> fjall::Result<()> {
    use fjall::{KeyspaceCreateOptions, SingleWriterTxDatabase};
    use std::sync::Arc;

    let folder = tempfile::tempdir()?;

    let db = SingleWriterTxDatabase::builder(&folder).open()?;

    let tree = db.keyspace(
        "default",
        KeyspaceCreateOptions::default()
            .compaction_strategy(Arc::new(fjall::compaction::Fifo::new(1, None))),
    )?;

    assert!(!tree.contains_key("a")?);

    let mut tx = db.write_tx();
    tx.insert(&tree, "a", "a");
    tx.commit()?;

    let snapshot = db.read_tx();
    assert!(snapshot.contains_key(&tree, "a")?);

    tree.inner().rotate_memtable_and_wait()?;
    std::thread::sleep(std::time::Duration::from_millis(500));

    assert!(snapshot.contains_key(&tree, "a")?);

    Ok(())
}
