#[test_log::test]
fn keyspace_lock() -> fjall::Result<()> {
    use fjall::Config;

    let folder = tempfile::tempdir()?;

    {
        let _keyspace = Config::new(&folder).open()?;
    }

    {
        let _keyspace = Config::new(&folder).open()?;
    }

    Ok(())
}
