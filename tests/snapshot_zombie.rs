use lsm_tree::Config;
use test_log::test;

const ITEM_COUNT: usize = 5;

#[test]
fn snapshot_zombie_memtable() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = Config::new(&folder).block_size(1_024).open()?;

    for x in 0..ITEM_COUNT as u64 {
        let key = x.to_be_bytes();
        tree.insert(key, "abc")?;
    }

    assert_eq!(tree.len()?, ITEM_COUNT);
    assert_eq!(tree.iter()?.into_iter().rev().count(), ITEM_COUNT);

    {
        let snapshot = tree.snapshot();
        assert_eq!(ITEM_COUNT, snapshot.len()?);
        assert_eq!(ITEM_COUNT, snapshot.iter()?.into_iter().rev().count());
    }

    for x in 0..ITEM_COUNT as u64 {
        let key = x.to_be_bytes();
        tree.remove(key)?;
    }

    assert_eq!(tree.len()?, 0);
    assert_eq!(tree.iter()?.into_iter().rev().count(), 0);

    {
        let snapshot = tree.snapshot();
        assert_eq!(0, snapshot.len()?);
        assert_eq!(0, snapshot.iter()?.into_iter().rev().count());
        assert_eq!(0, snapshot.prefix("")?.into_iter().count());
    }

    Ok(())
}

#[test]
fn snapshot_zombie_segment() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    {
        let tree = Config::new(&folder).block_size(1_024).open()?;

        for x in 0..ITEM_COUNT as u64 {
            let key = x.to_be_bytes();
            tree.insert(key, "abc")?;
        }

        tree.flush()?;
        tree.wait_for_memtable_flush()?;

        assert_eq!(tree.len()?, ITEM_COUNT);
        assert_eq!(tree.iter()?.into_iter().rev().count(), ITEM_COUNT);

        {
            let snapshot = tree.snapshot();
            assert_eq!(ITEM_COUNT, snapshot.len()?);
            assert_eq!(ITEM_COUNT, snapshot.iter()?.into_iter().rev().count());
        }

        for x in 0..ITEM_COUNT as u64 {
            let key = x.to_be_bytes();
            tree.remove(key)?;
        }

        tree.flush()?;
        tree.wait_for_memtable_flush()?;

        assert_eq!(tree.len()?, 0);
        assert_eq!(tree.iter()?.into_iter().rev().count(), 0);

        {
            let snapshot = tree.snapshot();
            assert_eq!(0, snapshot.len()?);
            assert_eq!(0, snapshot.iter()?.into_iter().rev().count());
            assert_eq!(0, snapshot.prefix("")?.into_iter().count());
        }
    }

    {
        let tree = Config::new(&folder).block_size(1_024).open()?;

        assert_eq!(tree.len()?, 0);
        assert_eq!(tree.iter()?.into_iter().rev().count(), 0);

        {
            let snapshot = tree.snapshot();
            assert_eq!(0, snapshot.len()?);
            assert_eq!(0, snapshot.iter()?.into_iter().rev().count());
            assert_eq!(0, snapshot.prefix("")?.into_iter().count());
        }
    }

    Ok(())
}
