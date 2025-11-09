#[test_log::test]
#[cfg(feature = "single_writer_tx")]
fn tx_ryow() -> fjall::Result<()> {
    use fjall::{KeyspaceCreateOptions, TxDatabase};

    let folder = tempfile::tempdir()?;

    let db = TxDatabase::builder(&folder).open()?;

    let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;

    let mut tx = db.write_tx();

    assert!(!tx.contains_key(&tree, "a")?);

    tx.insert(&tree, "a", "a");
    assert!(tx.contains_key(&tree, "a")?);

    tx.remove(&tree, "a");
    assert!(!tx.contains_key(&tree, "a")?);

    tx.insert(&tree, "a", "a");
    tx.insert(&tree, "a", "c");
    assert_eq!(b"c", &*tx.get(&tree, "a")?.unwrap());

    tx.remove(&tree, "a");
    assert!(!tx.contains_key(&tree, "a")?);

    Ok(())
}
