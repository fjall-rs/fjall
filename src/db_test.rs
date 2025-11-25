use crate::{AbstractTree, Database, KeyspaceCreateOptions, KvSeparationOptions};
use test_log::test;

#[test]
#[expect(clippy::unwrap_used)]
fn recover_sealed() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;

    for i in 0_u128..25 {
        let db = Database::create_or_recover(Database::builder(folder.path()).into_config())?;

        let tree = db.keyspace("default", || {
            KeyspaceCreateOptions::default().max_memtable_size(1_000)
        })?;

        assert_eq!(i, tree.len()?.try_into().unwrap());

        tree.insert(i.to_be_bytes(), i.to_be_bytes())?;
        assert_eq!(i + 1, tree.len()?.try_into().unwrap());

        tree.rotate_memtable_and_wait()?;
    }

    Ok(())
}

#[test]
#[expect(clippy::unwrap_used)]
fn recover_sealed_blob() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;

    for i in 0_u128..25 {
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
#[ignore = "broken in windows CI???"]
fn recover_sealed_pair_1() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;

    for i in 0_u128..25 {
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

#[test]
#[expect(clippy::unwrap_used)]
#[ignore = "restore force_flush"]
fn recover_sealed_pair_2() -> crate::Result<()> {
    use lsm_tree::AbstractTree;

    let folder = tempfile::tempdir()?;

    {
        let db = Database::create_or_recover(Database::builder(folder.path()).into_config())?;

        let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
        let tree2 = db.keyspace("default2", KeyspaceCreateOptions::default)?;

        tree.insert(0u8.to_be_bytes(), 0u8.to_be_bytes())?;
        tree2.insert(0u8.to_be_bytes(), 0u8.to_be_bytes())?;
        assert_eq!(1, tree.len()?.try_into().unwrap());
        assert_eq!(1, tree2.len()?.try_into().unwrap());
        assert_eq!(1, tree.tree.active_memtable().len());
        tree.rotate_memtable()?;
        assert_eq!(1, tree.tree.sealed_memtable_count());

        db.force_flush()?;
        assert_eq!(0, tree.tree.sealed_memtable_count());

        tree.insert(1u8.to_be_bytes(), 1u8.to_be_bytes())?;

        assert_eq!(2, tree.len()?.try_into().unwrap());
        assert_eq!(1, tree2.len()?.try_into().unwrap());
        assert_eq!(1, tree.tree.active_memtable().len());
        assert_eq!(2, db.journal_count());
    }

    {
        let db = Database::create_or_recover(Database::builder(folder.path()).into_config())?;

        let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
        let tree2 = db.keyspace("default2", KeyspaceCreateOptions::default)?;

        assert_eq!(2, tree.len()?.try_into().unwrap());
        assert_eq!(1, tree2.len()?.try_into().unwrap());
        assert_eq!(1, tree.tree.active_memtable().len());
        assert_eq!(2, db.journal_count());
    }

    Ok(())
}

#[test]
#[expect(clippy::unwrap_used)]
#[ignore = "restore force_flush"]
fn recover_sealed_pair_3() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;

    for i in 0_u128..25 {
        let db = Database::create_or_recover(Database::builder(folder.path()).into_config())?;

        let tree = db.keyspace("default", || {
            KeyspaceCreateOptions::default().max_memtable_size(1_024)
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

        log::info!("item now {i}");

        assert_eq!(1, tree2.tree.l0_run_count());

        assert_eq!(i + 1, tree.len()?.try_into().unwrap());
        assert_eq!(i + 1, tree2.len()?.try_into().unwrap());

        tree2.rotate_memtable()?;
        assert_eq!(1, tree2.tree.sealed_memtable_count());
        assert_eq!(0, tree2.tree.active_memtable().size());

        log::error!("-- MANUAL FLUSH --");
        db.force_flush()?;
        assert_eq!(0, tree2.tree.sealed_memtable_count());
    }

    Ok(())
}
