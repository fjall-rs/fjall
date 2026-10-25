use fjall::{Database, DurabilityLevel};
use test_log::test;

#[test]
fn db_journal_recovery_no_journal_files() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    {
        let _db = Database::builder(&folder).open()?;
    }

    for dirent in std::fs::read_dir(&folder)? {
        let path = dirent?.path();

        if path.extension().is_some_and(|ext| ext == "jnl") {
            std::fs::remove_file(path)?;
        }
    }

    {
        let db = Database::builder(&folder).open()?;
        let tree = db.keyspace("default", Default::default)?;
        tree.insert("hello", "world")?;
        db.persist(DurabilityLevel::SyncAll)?;
    }

    {
        let db = Database::builder(&folder).open()?;
        let tree = db.keyspace("default", Default::default)?;
        assert_eq!(Some("world".as_bytes().into()), tree.get("hello")?);
    }

    Ok(())
}
