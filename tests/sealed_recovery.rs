use fjall::{Database, KeyspaceCreateOptions};
use test_log::test;

#[test]
fn recover_sealed() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    for item in 0_u128..25 {
        let db = Database::create_or_recover(Database::builder(folder.path()).into_config())?;

        let tree = db.keyspace(
            "default",
            KeyspaceCreateOptions::default().max_memtable_size(1_000),
        )?;

        assert_eq!(item, tree.len()?.try_into().unwrap());

        tree.insert(item.to_be_bytes(), item.to_be_bytes())?;
        assert_eq!(item + 1, tree.len()?.try_into().unwrap());

        tree.rotate_memtable()?;
        db.force_flush()?;
    }

    Ok(())
}

#[test]
#[ignore = "fix at some point"]
fn recover_sealed_blob() -> fjall::Result<()> {
    todo!()

    // let folder = tempfile::tempdir()?;

    // for item in 0_u128..25 {
    //     let db = Database::create_or_recover(Database::builder(folder.path()).into_config())?;

    //     let tree = db.keyspace(
    //         "default",
    //         KeyspaceCreateOptions::default()
    //             .max_memtable_size(1_000)
    //             .with_kv_separation(KvSeparationOptions::default()),
    //     )?;

    //     assert_eq!(item, tree.len()?.try_into().unwrap());

    //     tree.insert(item.to_be_bytes(), item.to_be_bytes().repeat(1_000))?;
    //     assert_eq!(item + 1, tree.len()?.try_into().unwrap());

    //     tree.rotate_memtable()?;
    //     db.force_flush()?;
    // }

    // Ok(())
}

#[test]
#[ignore = "fix at some point"]
fn recover_sealed_pair_1() -> fjall::Result<()> {
    todo!()

    // let folder = tempfile::tempdir()?;

    // for item in 0_u128..25 {
    //     let db = Database::create_or_recover(Database::builder(folder.path()).into_config())?;

    //     let tree = db.keyspace(
    //         "default",
    //         KeyspaceCreateOptions::default().max_memtable_size(1_000),
    //     )?;
    //     let tree2 = db.keyspace(
    //         "default2",
    //         KeyspaceCreateOptions::default()
    //             .max_memtable_size(1_000)
    //             .with_kv_separation(KvSeparationOptions::default()),
    //     )?;

    //     assert_eq!(item, tree.len()?.try_into().unwrap());
    //     assert_eq!(item, tree2.len()?.try_into().unwrap());

    //     let mut batch = db.batch();
    //     batch.insert(&tree, item.to_be_bytes(), item.to_be_bytes());
    //     batch.insert(&tree2, item.to_be_bytes(), item.to_be_bytes().repeat(1_000));
    //     batch.commit()?;

    //     assert_eq!(item + 1, tree.len()?.try_into().unwrap());
    //     assert_eq!(item + 1, tree2.len()?.try_into().unwrap());

    //     tree.rotate_memtable()?;
    //     db.force_flush()?;
    // }

    // Ok(())
}

#[test]
fn recover_sealed_pair_2() -> fjall::Result<()> {
    use lsm_tree::AbstractTree;

    let folder = tempfile::tempdir()?;

    {
        let db = Database::create_or_recover(Database::builder(folder.path()).into_config())?;

        let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
        let tree2 = db.keyspace("default2", KeyspaceCreateOptions::default())?;

        tree.insert(0u8.to_be_bytes(), 0u8.to_be_bytes())?;
        tree2.insert(0u8.to_be_bytes(), 0u8.to_be_bytes())?;
        assert_eq!(1, tree.len()?.try_into().unwrap());
        assert_eq!(1, tree2.len()?.try_into().unwrap());

        // TODO: 3.0.0
        // assert_eq!(1, tree.tree.lock_active_memtable().len());

        tree.rotate_memtable()?;
        assert_eq!(1, tree.tree.sealed_memtable_count());

        db.force_flush()?;
        assert_eq!(0, tree.tree.sealed_memtable_count());

        tree.insert(1u8.to_be_bytes(), 1u8.to_be_bytes())?;

        assert_eq!(2, tree.len()?.try_into().unwrap());
        assert_eq!(1, tree2.len()?.try_into().unwrap());

        // TODO: 3.0.0
        // assert_eq!(1, tree.tree.lock_active_memtable().len());

        assert_eq!(2, db.journal_count());
    }

    {
        let db = Database::create_or_recover(Database::builder(folder.path()).into_config())?;

        let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
        let tree2 = db.keyspace("default2", KeyspaceCreateOptions::default())?;

        assert_eq!(2, tree.len()?.try_into().unwrap());
        assert_eq!(1, tree2.len()?.try_into().unwrap());

        // TODO: 3.0.0
        // assert_eq!(1, tree.tree.lock_active_memtable().len());

        assert_eq!(2, db.journal_count());
    }

    Ok(())
}

#[test]
#[ignore = "fix at some point"]
fn recover_sealed_pair_3() -> fjall::Result<()> {
    todo!()

    // let folder = tempfile::tempdir()?;

    // for item in 0_u128..25 {
    //     let db = Database::create_or_recover(Database::builder(folder.path()).into_config())?;

    //     let tree = db.keyspace(
    //         "default",
    //         KeyspaceCreateOptions::default().max_memtable_size(1_000),
    //     )?;
    //     let tree2 = db.keyspace(
    //         "default2",
    //         KeyspaceCreateOptions::default()
    //             .max_memtable_size(1_000)
    //             .with_kv_separation(KvSeparationOptions::default()),
    //     )?;

    //     assert_eq!(item, tree.len()?.try_into().unwrap());
    //     assert_eq!(item, tree2.len()?.try_into().unwrap());

    //     let mut batch = db.batch();
    //     batch.insert(&tree, item.to_be_bytes(), item.to_be_bytes());
    //     batch.insert(&tree2, item.to_be_bytes(), item.to_be_bytes().repeat(1_000));
    //     batch.commit()?;

    //     log::info!("item now {item}");

    //     use lsm_tree::AbstractTree;
    //     // assert_eq!(1, tree2.tree.l0_run_count());

    //     assert_eq!(item + 1, tree.len()?.try_into().unwrap());
    //     assert_eq!(item + 1, tree2.len()?.try_into().unwrap());

    //     tree2.rotate_memtable()?;
    //     assert_eq!(1, tree2.tree.sealed_memtable_count());
    //     assert!(tree2.tree.lock_active_memtable().is_empty());

    //     log::error!("-- MANUAL FLUSH --");
    //     db.force_flush()?;
    //     assert_eq!(0, tree2.tree.sealed_memtable_count());
    // }

    // Ok(())
}
