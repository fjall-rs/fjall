use lsm_tree::Config;
use tempfile::tempdir;
use test_log::test;

const ITEM_COUNT: usize = 1_000;

#[test]
fn tree_memtable_count() -> lsm_tree::Result<()> {
    let folder = tempdir()?.into_path();

    let tree = Config::new(folder).open()?;

    for x in 0..ITEM_COUNT as u64 {
        let key = x.to_be_bytes();
        let value = nanoid::nanoid!();
        tree.insert(key, value)?;
    }

    assert_eq!(tree.len()?, ITEM_COUNT);
    assert_eq!(
        tree.iter()?.into_iter().filter(|x| x.is_ok()).count(),
        ITEM_COUNT
    );
    assert_eq!(
        tree.iter()?.into_iter().rev().filter(|x| x.is_ok()).count(),
        ITEM_COUNT
    );

    Ok(())
}

#[test]
fn tree_flushed_count() -> lsm_tree::Result<()> {
    let folder = tempdir()?.into_path();

    let tree = Config::new(folder).open()?;

    for x in 0..ITEM_COUNT as u64 {
        let key = x.to_be_bytes();
        let value = nanoid::nanoid!();
        tree.insert(key, value)?;
    }

    tree.wait_for_memtable_flush()?;

    assert_eq!(tree.len()?, ITEM_COUNT);
    assert_eq!(
        tree.iter()?.into_iter().filter(|x| x.is_ok()).count(),
        ITEM_COUNT
    );
    assert_eq!(
        tree.iter()?.into_iter().rev().filter(|x| x.is_ok()).count(),
        ITEM_COUNT
    );

    Ok(())
}
