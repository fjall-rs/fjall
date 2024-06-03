use fjall::Config;
use test_log::test;

#[test]
fn keyspace_load_v1() -> fjall::Result<()> {
    let folder = "test_fixture/v1_keyspace";

    let result = Config::new(folder).open();

    matches!(
        result,
        Err(fjall::Error::InvalidVersion(Some(fjall::Version::V1)))
    );

    Ok(())
}

#[test]
fn keyspace_load_v1_corrupt_journal() -> fjall::Result<()> {
    let folder = "test_fixture/v1_keyspace_corrupt_journal";

    let result = Config::new(folder).open();

    matches!(
        result,
        Err(fjall::Error::InvalidVersion(Some(fjall::Version::V1)))
    );

    Ok(())
}
