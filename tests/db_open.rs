use fjall::{Database, KeyspaceCreateOptions, PersistMode};

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

#[test_log::test]
fn db_open_with_keyspace() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    {
        let db = Database::builder(&folder).open()?;
        let _keyspace = db.keyspace("hello", Default::default)?;
    }

    // DB should not be locked
    {
        let _db = Database::builder(&folder).open()?;
    }

    Ok(())
}

/// A failed creation attempt must not leave artifacts that make retrying impossible.
#[test_log::test]
fn db_open_retries_after_meta_tree_creation_failure() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    // An obstructed meta tree folder makes creation fail
    std::fs::create_dir_all(folder.path().join("keyspaces"))?;
    std::fs::File::create(folder.path().join("keyspaces").join("0"))?;

    assert!(Database::builder(&folder).open().is_err());

    std::fs::remove_file(folder.path().join("keyspaces").join("0"))?;

    {
        let db = Database::builder(&folder).open()?;
        let keyspace = db.keyspace("retry", KeyspaceCreateOptions::default)?;
        keyspace.insert("a", "b")?;
        db.persist(PersistMode::SyncAll)?;
    }

    let db = Database::builder(&folder).open()?;
    let keyspace = db.keyspace("retry", KeyspaceCreateOptions::default)?;
    assert_eq!(&*keyspace.get("a")?.expect("key should exist"), b"b");

    Ok(())
}
