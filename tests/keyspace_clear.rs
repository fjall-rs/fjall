#[test_log::test]
fn clear_recover() -> fjall::Result<()> {
    use fjall::{Database, KeyspaceCreateOptions};

    let folder = tempfile::tempdir()?;

    {
        let db = Database::builder(&folder).open()?;

        let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
        assert!(!tree.contains_key("a")?);

        tree.insert("a", "a")?;
        assert!(tree.contains_key("a")?);

        tree.clear()?;
        assert!(!tree.contains_key("a")?);
    }

    {
        let db = Database::builder(&folder).open()?;

        let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;

        assert!(!tree.contains_key("a")?);
    }

    Ok(())
}
