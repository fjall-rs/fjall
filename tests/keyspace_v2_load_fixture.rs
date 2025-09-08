use fjall::{Database, RecoveryError, Version};
use test_log::test;

#[test]
fn db_load_v2() -> fjall::Result<()> {
    let folder = "test_fixture/v2_keyspace";

    let result = Database::builder(folder).open();

    matches!(result, Err(fjall::Error::InvalidVersion(Some(Version::V2))));

    Ok(())
}

#[test]
fn db_load_v2_corrupt_journal() -> fjall::Result<()> {
    let folder = "test_fixture/v2_keyspace_corrupt_journal";

    let result = Database::builder(folder).open();

    matches!(
        result,
        Err(fjall::Error::JournalRecovery(
            RecoveryError::ChecksumMismatch
        )),
    );

    Ok(())
}
