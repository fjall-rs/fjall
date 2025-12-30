use crate::{Database, KeyspaceCreateOptions, KvSeparationOptions};
use test_log::test;

// TODO: investigate: flaky on macOS???
#[cfg(feature = "__internal_whitebox")]
#[test_log::test]
#[ignore = "restore"]
fn whitebox_db_drop() -> crate::Result<()> {
    use crate::Database;

    {
        let folder = tempfile::tempdir()?;

        assert_eq!(0, crate::drop::load_drop_counter());
        let db = Database::builder(&folder).open()?;
        assert_eq!(5, crate::drop::load_drop_counter());

        drop(db);
        assert_eq!(0, crate::drop::load_drop_counter());
    }

    {
        let folder = tempfile::tempdir()?;

        assert_eq!(0, crate::drop::load_drop_counter());
        let db = Database::builder(&folder).open()?;
        assert_eq!(5, crate::drop::load_drop_counter());

        let tree = db.keyspace("default", Default::default)?;
        assert_eq!(6, crate::drop::load_drop_counter());

        drop(tree);
        drop(db);
        assert_eq!(0, crate::drop::load_drop_counter());
    }

    {
        let folder = tempfile::tempdir()?;

        assert_eq!(0, crate::drop::load_drop_counter());
        let db = Database::builder(&folder).open()?;
        assert_eq!(5, crate::drop::load_drop_counter());

        let _tree = db.keyspace("default", Default::default)?;
        assert_eq!(6, crate::drop::load_drop_counter());

        let _tree2 = db.keyspace("different", Default::default)?;
        assert_eq!(7, crate::drop::load_drop_counter());
    }

    assert_eq!(0, crate::drop::load_drop_counter());

    Ok(())
}

#[cfg(feature = "__internal_whitebox")]
#[test_log::test]
#[ignore = "restore"]
fn whitebox_db_drop_2() -> crate::Result<()> {
    use crate::{Database, KeyspaceCreateOptions};

    let folder = tempfile::tempdir()?;

    {
        let db = Database::builder(&folder).open()?;

        let tree = db.keyspace("tree", KeyspaceCreateOptions::default)?;
        let tree2 = db.keyspace("tree1", KeyspaceCreateOptions::default)?;

        tree.insert("a", "a")?;
        tree2.insert("b", "b")?;

        tree.rotate_memtable_and_wait()?;
    }

    assert_eq!(0, crate::drop::load_drop_counter());

    Ok(())
}

#[test]
pub fn test_exotic_keyspace_names() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    let db = Database::builder(&folder).open()?;

    for name in ["hello$world", "hello#world", "hello.world", "hello_world"] {
        let tree = db.keyspace(name, KeyspaceCreateOptions::default)?;
        tree.insert("a", "a")?;
        assert_eq!(1, tree.len()?);
    }

    Ok(())
}

#[test]
#[expect(clippy::unwrap_used)]
fn recover_sealed() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;

    for i in 0_u128..3 {
        let db = Database::create_or_recover(Database::builder(folder.path()).into_config())?;

        let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;

        assert_eq!(i, tree.len()?.try_into().unwrap());

        tree.insert(i.to_be_bytes(), i.to_be_bytes())?;
        assert_eq!(i + 1, tree.len()?.try_into().unwrap());

        tree.rotate_memtable_and_wait()?;
    }

    Ok(())
}

#[test]
#[expect(clippy::unwrap_used)]
fn recover_sealed_order() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;

    {
        let db = Database::builder(folder.path())
            .worker_threads_unchecked(0)
            .open()?;

        let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;

        tree.insert("a", "a")?;
        tree.rotate_memtable()?;

        tree.insert("a", "b")?;
        tree.rotate_memtable()?;

        tree.insert("a", "c")?;
        tree.rotate_memtable()?;
    }

    {
        let db = Database::create_or_recover(Database::builder(folder.path()).into_config())?;

        let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;

        assert_eq!(b"c", &*tree.get("a")?.unwrap());
    }

    Ok(())
}

#[test]
#[expect(clippy::unwrap_used)]
fn recover_sealed_blob() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;

    for i in 0_u128..3 {
        let db = Database::create_or_recover(Database::builder(folder.path()).into_config())?;

        let tree = db.keyspace("default", || {
            KeyspaceCreateOptions::default()
                .max_memtable_size(1_000)
                .with_kv_separation(Some(KvSeparationOptions::default()))
        })?;

        assert_eq!(i, tree.len()?.try_into().unwrap());

        tree.insert(i.to_be_bytes(), i.to_be_bytes().repeat(1_024))?;
        assert_eq!(i + 1, tree.len()?.try_into().unwrap());

        tree.rotate_memtable_and_wait()?;
    }

    Ok(())
}

#[test]
#[expect(clippy::unwrap_used)]
fn recover_sealed_pair_1() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;

    for i in 0_u128..3 {
        let db = Database::create_or_recover(Database::builder(folder.path()).into_config())?;

        let tree = db.keyspace("default", || {
            KeyspaceCreateOptions::default().max_memtable_size(1_000)
        })?;
        let tree2 = db.keyspace("default2", || {
            KeyspaceCreateOptions::default()
                .max_memtable_size(1_000)
                .with_kv_separation(Some(KvSeparationOptions::default()))
        })?;

        assert_eq!(i, tree.len()?.try_into().unwrap());
        assert_eq!(i, tree2.len()?.try_into().unwrap());

        let mut batch = db.batch();
        batch.insert(&tree, i.to_be_bytes(), i.to_be_bytes());
        batch.insert(&tree2, i.to_be_bytes(), i.to_be_bytes().repeat(1_024));
        batch.commit()?;

        assert_eq!(i + 1, tree.len()?.try_into().unwrap());
        assert_eq!(i + 1, tree2.len()?.try_into().unwrap());

        tree.rotate_memtable_and_wait()?;
    }

    Ok(())
}
