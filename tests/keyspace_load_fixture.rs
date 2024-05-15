use fjall::Config;
use test_log::test;

#[test]
fn keyspace_load_v1() -> fjall::Result<()> {
    let folder = "test_fixture/v1_keyspace";

    let keyspace = Config::new(folder).open()?;

    let a = keyspace.open_partition("a", Default::default())?;
    let b = keyspace.open_partition("b", Default::default())?;
    let c = keyspace.open_partition("c", Default::default())?;

    assert_eq!(3, keyspace.partition_count());

    assert_eq!(1, a.tree.first_level_segment_count());
    assert_eq!(8, a.len()?);

    assert_eq!(1, b.tree.first_level_segment_count());
    assert_eq!(4, b.len()?);

    assert_eq!(0, c.tree.first_level_segment_count());
    assert_eq!(4, c.len()?);

    // TODO: call Keyspace::verify
    // needs to call Tree::verify on every partition and verify *all* journals

    Ok(())
}

#[test]
fn keyspace_load_v1_corrupt_journal() -> fjall::Result<()> {
    let folder = "test_fixture/v1_keyspace_corrupt_journal";

    let result = Config::new(folder).open();

    matches!(
        result,
        Err(fjall::Error::JournalRecovery(
            fjall::RecoveryError::CrcCheck
        ))
    );

    Ok(())
}
