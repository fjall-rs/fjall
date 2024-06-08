use fjall::{Config, PartitionCreateOptions};
use test_log::test;

#[test]
fn tx_ryow() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    let keyspace = Config::new(&folder).open_transactional()?;

    let tree = keyspace.open_partition("default", PartitionCreateOptions::default())?;

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
