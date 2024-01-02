use fjall::{Config, PartitionConfig};
use test_log::test;

#[test]
fn batch_simple() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    let folder = tempfile::tempdir()?;
    let keyspace = Config::new(folder).open()?;
    let partition = keyspace.open_partition("default", PartitionConfig::default())?;
    let mut batch = keyspace.batch();

    assert_eq!(partition.len()?, 0);
    batch.insert(&partition, "1", "abc");
    batch.insert(&partition, "3", "abc");
    batch.insert(&partition, "5", "abc");

    assert_eq!(partition.len()?, 0);

    batch.commit()?;
    assert_eq!(partition.len()?, 3);

    Ok(())
}
