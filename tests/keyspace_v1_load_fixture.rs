use fjall::Database;
use test_log::test;

#[test]
fn db_load_v1() -> fjall::Result<()> {
    let folder = "test_fixture/v1_keyspace";

    let result = Database::builder(folder).open();

    matches!(
        result,
        Err(fjall::Error::InvalidVersion(Some(fjall::FormatVersion::V1)))
    );

    Ok(())
}

#[test]
fn db_load_v1_corrupt_journal() -> fjall::Result<()> {
    let folder = "test_fixture/v1_keyspace_corrupt_journal";

    let result = Database::builder(folder).open();

    matches!(
        result,
        Err(fjall::Error::InvalidVersion(Some(fjall::FormatVersion::V1)))
    );

    Ok(())
}
