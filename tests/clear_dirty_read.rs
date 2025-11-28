use fjall::Readable;

#[test_log::test]
fn clear_dirty_read() -> fjall::Result<()> {
    use fjall::{KeyspaceCreateOptions, SingleWriterTxDatabase};

    let folder = tempfile::tempdir()?;

    let db = SingleWriterTxDatabase::builder(&folder).open()?;

    let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;

    assert!(!tree.contains_key("a")?);

    let mut tx = db.write_tx();
    tx.insert(&tree, "a", "a");
    tx.commit()?;

    let snapshot = db.read_tx();
    assert!(snapshot.contains_key(&tree, "a")?);

    tree.inner().clear()?;

    assert!(snapshot.contains_key(&tree, "a")?);

    assert!(!tree.contains_key("a")?);
    assert_eq!(0, tree.approximate_len());

    {
        let snapshot = db.read_tx();
        assert!(!snapshot.contains_key(&tree, "a")?);
    }

    Ok(())
}
