use fjall::Config;
use test_log::test;

#[test]
fn keyspace_recover_empty() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    for _ in 0..10 {
        let _keyspace = Config::new(&folder).open()?;
        assert_eq!(0, _keyspace.partition_count());
    }

    Ok(())
}
