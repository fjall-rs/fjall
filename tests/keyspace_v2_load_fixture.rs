use fjall::{Config, KvSeparationOptions, PartitionCreateOptions, RecoveryError};
use test_log::test;

#[test]
fn keyspace_load_v2() -> fjall::Result<()> {
    let folder = "test_fixture/v2_keyspace";

    let keyspace = Config::new(folder).open()?;
    let tree1 = keyspace.open_partition("default1", PartitionCreateOptions::default())?;
    let tree2 = keyspace.open_partition(
        "default2",
        PartitionCreateOptions::default().with_kv_separation(KvSeparationOptions::default()),
    )?;

    assert_eq!(6, tree1.len()?);
    assert_eq!(6, tree2.len()?);

    Ok(())
}

#[test]
fn keyspace_load_v2_corrupt_journal() -> fjall::Result<()> {
    let folder = "test_fixture/v2_keyspace_corrupt_journal";

    let result = Config::new(folder).open();
    matches!(
        result,
        Err(fjall::Error::JournalRecovery(
            RecoveryError::ChecksumMismatch
        )),
    );

    Ok(())
}
