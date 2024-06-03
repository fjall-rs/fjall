use fjall::{Config, PartitionCreateOptions};
use test_log::test;

#[test]
fn batch_simple() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    let keyspace = Config::new(folder).open()?;
    let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
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

// TODO: 2.0.0
#[test]
#[ignore = "need to fix batch commit"]
fn blob_batch_simple() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    let keyspace = Config::new(folder).open()?;
    let partition = keyspace.open_partition(
        "default",
        PartitionCreateOptions::default().use_kv_separation(),
    )?;
    let mut batch = keyspace.batch();

    assert_eq!(partition.len()?, 0);
    batch.insert(&partition, "1", "oxygen".repeat(128_000));
    batch.insert(&partition, "3", "abc");
    batch.insert(&partition, "5", "abc");
    assert_eq!(partition.len()?, 0);

    batch.commit()?;
    assert_eq!(partition.len()?, 3);

    Ok(())
}
