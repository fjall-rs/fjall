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

#[test]
fn blob_batch_simple() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    let keyspace = Config::new(folder).open()?;
    let partition = keyspace.open_partition(
        "default",
        PartitionCreateOptions::default().use_kv_separation(true),
    )?;

    let blob = "oxygen".repeat(128_000);

    let mut batch = keyspace.batch();

    assert_eq!(partition.len()?, 0);
    batch.insert(&partition, "1", &blob);
    batch.insert(&partition, "3", "abc");
    batch.insert(&partition, "5", "abc");
    assert_eq!(partition.len()?, 0);

    batch.commit()?;
    assert_eq!(partition.len()?, 3);

    assert_eq!(&*partition.get("1")?.unwrap(), blob.as_bytes());

    Ok(())
}
