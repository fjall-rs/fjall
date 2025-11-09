// TODO: investigate: flaky on macOS???
#[cfg(feature = "__internal_whitebox")]
#[test_log::test]
#[ignore = "3.0.0 restore"]
fn whitebox_db_drop() -> fjall::Result<()> {
    use fjall::Database;

    {
        let folder = tempfile::tempdir()?;

        assert_eq!(0, fjall::drop::load_drop_counter());
        let db = Database::builder(&folder).open()?;
        assert_eq!(5, fjall::drop::load_drop_counter());

        drop(db);
        assert_eq!(0, fjall::drop::load_drop_counter());
    }

    {
        let folder = tempfile::tempdir()?;

        assert_eq!(0, fjall::drop::load_drop_counter());
        let db = Database::builder(&folder).open()?;
        assert_eq!(5, fjall::drop::load_drop_counter());

        let tree = db.keyspace("default", Default::default())?;
        assert_eq!(6, fjall::drop::load_drop_counter());

        drop(tree);
        drop(db);
        assert_eq!(0, fjall::drop::load_drop_counter());
    }

    {
        let folder = tempfile::tempdir()?;

        assert_eq!(0, fjall::drop::load_drop_counter());
        let db = Database::builder(&folder).open()?;
        assert_eq!(5, fjall::drop::load_drop_counter());

        let _tree = db.keyspace("default", Default::default())?;
        assert_eq!(6, fjall::drop::load_drop_counter());

        let _tree2 = db.keyspace("different", Default::default())?;
        assert_eq!(7, fjall::drop::load_drop_counter());
    }

    assert_eq!(0, fjall::drop::load_drop_counter());

    Ok(())
}

#[cfg(feature = "__internal_whitebox")]
#[test_log::test]
#[ignore = "3.0.0 restore"]
fn whitebox_db_drop_2() -> fjall::Result<()> {
    use fjall::{Database, KeyspaceCreateOptions};

    let folder = tempfile::tempdir()?;

    {
        let db = Database::builder(&folder).open()?;

        let tree = db.keyspace("tree", KeyspaceCreateOptions::default())?;
        let tree2 = db.keyspace("tree1", KeyspaceCreateOptions::default())?;

        tree.insert("a", "a")?;
        tree2.insert("b", "b")?;

        tree.rotate_memtable_and_wait()?;
    }

    assert_eq!(0, fjall::drop::load_drop_counter());

    Ok(())
}
