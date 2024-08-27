#[test_log::test]
#[cfg(feature = "single_writer_tx")]
fn tx_ryow() -> fjall::Result<()> {
    use fjall::{Config, PartitionOptions};

    let folder = tempfile::tempdir()?;

    let keyspace = Config::new(&folder).open_transactional()?;

    let tree = keyspace.open_partition("default", PartitionOptions::default())?;

    let mut tx = keyspace.write_tx();

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
