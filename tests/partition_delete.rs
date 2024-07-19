use fjall::{Config, PartitionCreateOptions};
use test_log::test;

const ITEM_COUNT: usize = 100;

#[test]
fn partition_delete() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    let path;

    // NOTE: clippy bug
    #[allow(unused_assignments)]
    {
        let keyspace = Config::new(&folder).open()?;

        let tree = keyspace.open_partition("default", PartitionCreateOptions::default())?;
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

        assert_eq!(tree.len()?, ITEM_COUNT * 2);
        assert_eq!(tree.iter().flatten().count(), ITEM_COUNT * 2);
        assert_eq!(tree.iter().rev().flatten().count(), ITEM_COUNT * 2);
    }

    for _ in 0..10 {
        let keyspace = Config::new(&folder).open()?;

        let tree = keyspace.open_partition("default", PartitionCreateOptions::default())?;

        assert_eq!(tree.len()?, ITEM_COUNT * 2);
        assert_eq!(tree.iter().flatten().count(), ITEM_COUNT * 2);
        assert_eq!(tree.iter().rev().flatten().count(), ITEM_COUNT * 2);

        assert!(path.try_exists()?);
    }

    {
        let keyspace = Config::new(&folder).open()?;

        let tree = keyspace.open_partition("default", PartitionCreateOptions::default())?;

        assert!(path.try_exists()?);

        keyspace.delete_partition(tree)?;

        assert!(!path.try_exists()?);
    }

    {
        let _keyspace = Config::new(&folder).open()?;
        assert!(!path.try_exists()?);
    }

    Ok(())
}

#[test]
#[cfg(feature = "single_writer_tx")]
fn tx_partition_delete() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    let path;

    // NOTE: clippy bug
    #[allow(unused_assignments)]
    {
        let keyspace = Config::new(&folder).open_transactional()?;

        let tree = keyspace.open_partition("default", PartitionCreateOptions::default())?;
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

        assert_eq!(keyspace.read_tx().len(&tree)?, ITEM_COUNT * 2);
        assert_eq!(
            keyspace.read_tx().iter(&tree).flatten().count(),
            ITEM_COUNT * 2
        );
        assert_eq!(
            keyspace.read_tx().iter(&tree).rev().flatten().count(),
            ITEM_COUNT * 2
        );
    }

    for _ in 0..10 {
        let keyspace = Config::new(&folder).open_transactional()?;

        let tree = keyspace.open_partition("default", PartitionCreateOptions::default())?;

        assert_eq!(keyspace.read_tx().len(&tree)?, ITEM_COUNT * 2);
        assert_eq!(
            keyspace.read_tx().iter(&tree).flatten().count(),
            ITEM_COUNT * 2
        );
        assert_eq!(
            keyspace.read_tx().iter(&tree).rev().flatten().count(),
            ITEM_COUNT * 2
        );

        assert!(path.try_exists()?);
    }

    {
        let keyspace = Config::new(&folder).open_transactional()?;

        let tree = keyspace.open_partition("default", PartitionCreateOptions::default())?;

        assert!(path.try_exists()?);

        keyspace.delete_partition(tree)?;

        assert!(!path.try_exists()?);
    }

    {
        let _keyspace = Config::new(&folder).open_transactional()?;
        assert!(!path.try_exists()?);
    }

    Ok(())
}

#[test]
fn partition_deletion_and_reopening_behavior() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    let keyspace = Config::new(&folder).open()?;
    let partition = keyspace.open_partition("default", Default::default())?;

    keyspace.delete_partition(partition.clone())?;

    // NOTE: Partition is marked as deleted but still referenced, so it's not cleaned up
    assert!(matches!(
        keyspace.open_partition("default", Default::default()),
        Err(fjall::Error::PartitionDeleted)
    ));

    // NOTE: Remove last handle, will drop partition folder, allowing us to recreate again
    drop(partition);

    assert!(keyspace
        .open_partition("default", Default::default())
        .is_ok());

    Ok(())
}
