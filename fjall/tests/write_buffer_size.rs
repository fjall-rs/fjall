use fjall::{Config, PartitionCreateOptions};
use test_log::test;

#[test]
fn write_buffer_size_after_insert() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    let keyspace = Config::new(&folder).open()?;

    let tree = keyspace.open_partition("default", PartitionCreateOptions::default())?;
    assert_eq!(0, keyspace.write_buffer_size());

    tree.insert("asd", "def")?;

    let write_buffer_size_after = keyspace.write_buffer_size();
    assert!(write_buffer_size_after > 0);

    let mut batch = keyspace.batch();
    batch.insert(&tree, "dsa", "qwe");
    batch.commit()?;

    let write_buffer_size_after_batch = keyspace.write_buffer_size();
    assert!(write_buffer_size_after_batch > write_buffer_size_after);

    Ok(())
}
