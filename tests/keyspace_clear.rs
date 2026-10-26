use fjall::{Database, DurabilityLevel::Buffer, KeyspaceCreateOptions};

#[test_log::test]
fn clear_recover() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    {
        let db = Database::builder(&folder).open()?;

        let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
        assert!(tree.is_empty()?);

        tree.insert("a", "a", Buffer)?;
        assert!(tree.contains_key("a")?);

        tree.clear()?;
        assert!(tree.is_empty()?);
    }

    {
        let db = Database::builder(&folder).open()?;

        let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;

        assert!(tree.is_empty()?);
    }

    Ok(())
}

#[test_log::test]
fn clear_recover_multi_tree() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    {
        let db = Database::builder(&folder).open()?;

        let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
        let other = db.keyspace("other", KeyspaceCreateOptions::default)?;
        assert!(tree.is_empty()?);

        tree.insert("a", "a", Buffer)?;
        assert!(tree.contains_key("a")?);

        tree.clear()?;
        assert!(tree.is_empty()?);

        other.insert("a", "z", Buffer)?;
    }

    {
        let db = Database::builder(&folder).open()?;

        let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
        assert!(tree.is_empty()?);

        let other = db.keyspace("other", KeyspaceCreateOptions::default)?;
        other.clear()?;
        assert!(other.is_empty()?);
    }

    {
        let db = Database::builder(&folder).open()?;

        let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
        let other = db.keyspace("other", KeyspaceCreateOptions::default)?;
        assert!(tree.is_empty()?);
        assert!(other.is_empty()?);

        tree.insert("a", "a", Buffer)?;
        other.clear()?;
    }

    {
        let db = Database::builder(&folder).open()?;

        let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
        let other = db.keyspace("other", KeyspaceCreateOptions::default)?;
        assert!(tree.contains_key("a")?);
        assert!(other.is_empty()?);
    }

    Ok(())
}
