use fjall::Config;
use test_log::test;

#[test]
fn keyspace_journal_recovery_mac_ds_store() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    {
        let _keyspace = Config::new(&folder).open()?;
    }

    std::fs::File::create(folder.path().join("journals").join(".DS_Store"))?;

    {
        let _keyspace = Config::new(&folder).open()?;
    }

    Ok(())
}

#[test]
fn keyspace_journal_recovery_mac_underscore_file() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    {
        let _keyspace = Config::new(&folder).open()?;
    }

    std::fs::File::create(folder.path().join("journals").join("._0"))?;

    {
        let _keyspace = Config::new(&folder).open()?;
    }

    Ok(())
}
