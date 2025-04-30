use fjall::{Config, Keyspace, KvSeparationOptions, PartitionCreateOptions};
use test_log::test;

#[test]
fn recover_sealed() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    for item in 0_u128..100 {
        let config = Config::new(&folder);
        let keyspace = Keyspace::create_or_recover(config)?;

        let tree = keyspace.open_partition(
            "default",
            PartitionCreateOptions::default().max_memtable_size(1_000),
        )?;

        assert_eq!(item, tree.len()?.try_into().unwrap());

        tree.insert(item.to_be_bytes(), item.to_be_bytes())?;
        assert_eq!(item + 1, tree.len()?.try_into().unwrap());

        tree.rotate_memtable()?;
        keyspace.force_flush()?;
    }

    Ok(())
}

#[test]
fn recover_sealed_blob() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    for item in 0_u128..100 {
        let config = Config::new(&folder);
        let keyspace = Keyspace::create_or_recover(config)?;

        let tree = keyspace.open_partition(
            "default",
            PartitionCreateOptions::default()
                .max_memtable_size(1_000)
                .with_kv_separation(KvSeparationOptions::default()),
        )?;

        assert_eq!(item, tree.len()?.try_into().unwrap());

        tree.insert(item.to_be_bytes(), item.to_be_bytes().repeat(1_000))?;
        assert_eq!(item + 1, tree.len()?.try_into().unwrap());

        tree.rotate_memtable()?;
        keyspace.force_flush()?;
    }

    Ok(())
}

#[test]
fn recover_sealed_pair_1() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    for item in 0_u128..100 {
        let config = Config::new(&folder);
        let keyspace = Keyspace::create_or_recover(config)?;

        let tree = keyspace.open_partition(
            "default",
            PartitionCreateOptions::default().max_memtable_size(1_000),
        )?;
        let tree2 = keyspace.open_partition(
            "default2",
            PartitionCreateOptions::default()
                .max_memtable_size(1_000)
                .with_kv_separation(KvSeparationOptions::default()),
        )?;

        assert_eq!(item, tree.len()?.try_into().unwrap());
        assert_eq!(item, tree2.len()?.try_into().unwrap());

        let mut batch = keyspace.batch();
        batch.insert(&tree, item.to_be_bytes(), item.to_be_bytes());
        batch.insert(&tree2, item.to_be_bytes(), item.to_be_bytes().repeat(1_000));
        batch.commit()?;

        assert_eq!(item + 1, tree.len()?.try_into().unwrap());
        assert_eq!(item + 1, tree2.len()?.try_into().unwrap());

        tree.rotate_memtable()?;
        keyspace.force_flush()?;
    }

    Ok(())
}

#[test]
fn recover_sealed_pair_2() -> fjall::Result<()> {
    use lsm_tree::AbstractTree;

    let folder = tempfile::tempdir()?;

    {
        let config = Config::new(&folder);
        let keyspace = Keyspace::create_or_recover(config)?;

        let tree = keyspace.open_partition("default", PartitionCreateOptions::default())?;
        let tree2 = keyspace.open_partition("default2", PartitionCreateOptions::default())?;

        tree.insert(0u8.to_be_bytes(), 0u8.to_be_bytes())?;
        tree2.insert(0u8.to_be_bytes(), 0u8.to_be_bytes())?;
        assert_eq!(1, tree.len()?.try_into().unwrap());
        assert_eq!(1, tree2.len()?.try_into().unwrap());
        assert_eq!(1, tree.tree.lock_active_memtable().len());

        tree.rotate_memtable()?;
        assert_eq!(1, tree.tree.sealed_memtable_count());

        keyspace.force_flush()?;
        assert_eq!(0, tree.tree.sealed_memtable_count());

        tree.insert(1u8.to_be_bytes(), 1u8.to_be_bytes())?;

        assert_eq!(2, tree.len()?.try_into().unwrap());
        assert_eq!(1, tree2.len()?.try_into().unwrap());
        assert_eq!(1, tree.tree.lock_active_memtable().len());

        assert_eq!(2, keyspace.journal_count());
    }

    {
        let config = Config::new(&folder);
        let keyspace = Keyspace::create_or_recover(config)?;

        let tree = keyspace.open_partition("default", PartitionCreateOptions::default())?;
        let tree2 = keyspace.open_partition("default2", PartitionCreateOptions::default())?;

        assert_eq!(2, tree.len()?.try_into().unwrap());
        assert_eq!(1, tree2.len()?.try_into().unwrap());
        assert_eq!(1, tree.tree.lock_active_memtable().len());

        assert_eq!(2, keyspace.journal_count());
    }

    Ok(())
}

#[test]
fn recover_sealed_pair_3() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    for item in 0_u128..100 {
        let config = Config::new(&folder);
        let keyspace = Keyspace::create_or_recover(config)?;

        let tree = keyspace.open_partition(
            "default",
            PartitionCreateOptions::default().max_memtable_size(1_000),
        )?;
        let tree2 = keyspace.open_partition(
            "default2",
            PartitionCreateOptions::default()
                .max_memtable_size(1_000)
                .with_kv_separation(KvSeparationOptions::default()),
        )?;

        assert_eq!(item, tree.len()?.try_into().unwrap());
        assert_eq!(item, tree2.len()?.try_into().unwrap());

        let mut batch = keyspace.batch();
        batch.insert(&tree, item.to_be_bytes(), item.to_be_bytes());
        batch.insert(&tree2, item.to_be_bytes(), item.to_be_bytes().repeat(1_000));
        batch.commit()?;

        log::info!("item now {item}");

        use lsm_tree::AbstractTree;
        assert!(tree2.tree.l0_run_count() == 1);

        assert_eq!(item + 1, tree.len()?.try_into().unwrap());
        assert_eq!(item + 1, tree2.len()?.try_into().unwrap());

        tree2.rotate_memtable()?;
        assert_eq!(1, tree2.tree.sealed_memtable_count());
        assert!(tree2.tree.lock_active_memtable().is_empty());

        log::error!("-- MANUAL FLUSH --");
        keyspace.force_flush()?;
        assert_eq!(0, tree2.tree.sealed_memtable_count());
    }

    Ok(())
}
