use fjall::{Database, KeyspaceCreateOptions, TxDatabase};
use test_log::test;

const ITEM_COUNT: usize = 10;

#[test]
fn keyspace_delete() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    let path;

    // NOTE: clippy bug
    #[allow(unused_assignments)]
    {
        let db = Database::builder(&folder).open()?;

        let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
        path = tree.path().to_path_buf();

        assert!(path.try_exists()?);

        for x in 0..ITEM_COUNT as u64 {
            let key = x.to_be_bytes();
            let value = nanoid::nanoid!();
            tree.insert(key, value.as_bytes())?;
        }

        for x in 0..ITEM_COUNT as u64 {
            let key: [u8; 8] = (x + ITEM_COUNT as u64).to_be_bytes();
            let value = nanoid::nanoid!();
            tree.insert(key, value.as_bytes())?;
        }

        assert_eq!(tree.len()?, ITEM_COUNT * 2);
        assert_eq!(tree.iter().flat_map(|x| x.key()).count(), ITEM_COUNT * 2);
        assert_eq!(
            tree.iter().rev().flat_map(|x| x.key()).count(),
            ITEM_COUNT * 2,
        );
    }

    for _ in 0..10 {
        let db = Database::builder(&folder).open()?;

        let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;

        assert_eq!(tree.len()?, ITEM_COUNT * 2);
        assert_eq!(tree.iter().flat_map(|x| x.key()).count(), ITEM_COUNT * 2);
        assert_eq!(
            tree.iter().rev().flat_map(|x| x.key()).count(),
            ITEM_COUNT * 2,
        );

        assert!(path.try_exists()?);
    }

    {
        let db = Database::builder(&folder).open()?;

        let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;

        assert!(path.try_exists()?);

        db.delete_keyspace(tree)?;

        assert!(!path.try_exists()?);
    }

    {
        let _db = Database::builder(&folder).open()?;
        assert!(!path.try_exists()?);
    }

    Ok(())
}

#[test]
#[cfg(feature = "single_writer_tx")]
fn tx_keyspace_delete() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    let path;

    // NOTE: clippy bug
    #[allow(unused_assignments)]
    {
        let db = TxDatabase::builder(&folder).open()?;

        let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
        path = tree.path();

        assert!(path.try_exists()?);

        for x in 0..ITEM_COUNT as u64 {
            let key = x.to_be_bytes();
            let value = nanoid::nanoid!();
            tree.insert(key, value.as_bytes())?;
        }

        for x in 0..ITEM_COUNT as u64 {
            let key: [u8; 8] = (x + ITEM_COUNT as u64).to_be_bytes();
            let value = nanoid::nanoid!();
            tree.insert(key, value.as_bytes())?;
        }

        assert_eq!(db.read_tx().len(&tree)?, ITEM_COUNT * 2);
        assert_eq!(
            db.read_tx().iter(&tree).flat_map(|x| x.key()).count(),
            ITEM_COUNT * 2,
        );
        assert_eq!(
            db.read_tx().iter(&tree).rev().flat_map(|x| x.key()).count(),
            ITEM_COUNT * 2,
        );
    }

    for _ in 0..5 {
        let db = TxDatabase::builder(&folder).open()?;

        let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;

        assert_eq!(db.read_tx().len(&tree)?, ITEM_COUNT * 2);
        assert_eq!(
            db.read_tx().iter(&tree).flat_map(|x| x.key()).count(),
            ITEM_COUNT * 2,
        );
        assert_eq!(
            db.read_tx().iter(&tree).rev().flat_map(|x| x.key()).count(),
            ITEM_COUNT * 2,
        );

        assert!(path.try_exists()?);
    }

    {
        let db = TxDatabase::builder(&folder).open()?;

        {
            let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;

            assert!(path.try_exists()?);

            db.inner().delete_keyspace(tree.inner().clone())?;
        }

        assert!(!path.try_exists()?);
    }

    {
        let _db = Database::builder(&folder).open()?;
        assert!(!path.try_exists()?);
    }

    Ok(())
}

#[test]
fn keyspace_delete_and_reopening_behavior() -> fjall::Result<()> {
    let keyspace_name = "default";
    let folder = tempfile::tempdir()?;

    let keyspace_exists = |id: u64| -> std::io::Result<bool> {
        folder
            .path()
            .join("keyspaces")
            .join(id.to_string())
            .try_exists()
    };

    let db = Database::builder(&folder).open()?;
    assert!(!keyspace_exists(1)?);

    let keyspace = db.keyspace(keyspace_name, Default::default())?;
    assert!(keyspace_exists(1)?);

    db.delete_keyspace(keyspace.clone())?;
    assert!(keyspace_exists(1)?);

    assert!(db.keyspace("default", Default::default()).is_ok());
    assert!(keyspace_exists(2)?);

    Ok(())
}
