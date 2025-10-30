#[test_log::test]
#[cfg(feature = "single_writer_tx")]
fn fifo_dirty_read() -> fjall::Result<()> {
    use std::sync::Arc;

    use fjall::{KeyspaceCreateOptions, TxDatabase};

    let folder = tempfile::tempdir()?;

    let db = TxDatabase::builder(&folder).open()?;

    let tree = db.keyspace(
        "default",
        KeyspaceCreateOptions::default()
            .compaction_strategy(Arc::new(fjall::compaction::Fifo::new(1, None))),
    )?;

    let mut tx = db.write_tx();
    tx.insert(&tree, "a", "a");
    tx.commit()?;

    let snapshot = db.read_tx();
    assert!(snapshot.contains_key(&tree, "a")?);

    tree.inner().rotate_memtable_and_wait()?;

    assert!(snapshot.contains_key(&tree, "a")?);

    Ok(())
}
