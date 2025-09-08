use fjall::Database;
use test_log::test;

#[test]
fn db_keyspaces_recovery_mac_ds_store() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    {
        let _db = Database::builder(&folder).open()?;
    }

    std::fs::File::create(folder.path().join("keyspaces").join(".DS_Store"))?;

    {
        let _db = Database::builder(&folder).open()?;
    }

    Ok(())
}

#[test]
fn db_keyspaces_recovery_mac_underscore() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    {
        let _db = Database::builder(&folder).open()?;
    }

    std::fs::File::create(folder.path().join("keyspaces").join("._0"))?;

    {
        let _db = Database::builder(&folder).open()?;
    }

    Ok(())
}
