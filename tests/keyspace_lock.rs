use fjall::Database;

#[test_log::test]
fn db_lock() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    {
        let _db = Database::builder(&folder).open()?;
    }

    {
        let _db = Database::builder(&folder).open()?;
    }

    Ok(())
}
