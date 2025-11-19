use fjall::Database;

#[test_log::test]
fn db_open() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    {
        let _db = Database::builder(&folder).open()?;
    }

    // DB should not be locked
    {
        let _db = Database::builder(&folder).open()?;
    }

    Ok(())
}
