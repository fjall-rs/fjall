use lsm_tree::Config;
use test_log::test;

#[test]
fn tree_shadowing_upsert() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?.into_path();

    let tree = Config::new(folder).open()?;

    let key = "1";
    let value = b"oldvalue";

    assert_eq!(tree.len()?, 0);
    tree.insert(key, *value)?;
    assert_eq!(tree.len()?, 1);
    assert_eq!(tree.get(key)?.map(|x| x.value), Some(value.to_vec()));

    tree.wait_for_memtable_flush()?;
    assert_eq!(tree.len()?, 1);
    assert_eq!(tree.get(key)?.map(|x| x.value), Some(value.to_vec()));

    let value = b"newvalue";

    tree.insert(key, *value)?;
    assert_eq!(tree.len()?, 1);
    assert_eq!(tree.get(key)?.map(|x| x.value), Some(value.to_vec()));

    tree.wait_for_memtable_flush()?;
    assert_eq!(tree.len()?, 1);
    assert_eq!(tree.get(key)?.map(|x| x.value), Some(value.to_vec()));

    Ok(())
}

#[test]
fn tree_shadowing_delete() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?.into_path();

    let tree = Config::new(folder).open().unwrap();

    let key = "1";
    let value = b"oldvalue";

    assert_eq!(tree.len()?, 0);
    tree.insert(key, *value)?;
    assert_eq!(tree.len()?, 1);
    assert_eq!(tree.get(key)?.map(|x| x.value), Some(value.to_vec()));

    tree.wait_for_memtable_flush()?;
    assert_eq!(tree.len()?, 1);
    assert_eq!(tree.get(key)?.map(|x| x.value), Some(value.to_vec()));

    tree.remove(key)?;
    eprint!("removed key {:?}", key);
    assert_eq!(tree.len()?, 0);
    assert!(tree.get(key)?.is_none());

    tree.wait_for_memtable_flush()?;
    assert_eq!(tree.len()?, 0);
    assert!(tree.get(key)?.is_none());

    Ok(())
}

#[test]
fn tree_shadowing_range() -> lsm_tree::Result<()> {
    const ITEM_COUNT: usize = 10_000;

    let folder = tempfile::tempdir()?.into_path();

    let tree = Config::new(folder).block_size(1_024).open()?;

    for x in 0..ITEM_COUNT as u64 {
        let key = x.to_be_bytes();
        let value = "old";
        tree.insert(key, value)?;
    }

    tree.wait_for_memtable_flush()?;

    assert_eq!(tree.len()?, ITEM_COUNT);
    assert!(tree
        .iter()?
        .into_iter()
        .all(|x| x.unwrap().value == "old".as_bytes().to_vec()));

    for x in 0..ITEM_COUNT as u64 {
        let key = x.to_be_bytes();
        let value = "new";
        tree.insert(key, value)?;
    }

    assert_eq!(tree.len()?, ITEM_COUNT);
    assert!(tree
        .iter()?
        .into_iter()
        .all(|x| x.unwrap().value == "new".as_bytes().to_vec()));

    tree.wait_for_memtable_flush()?;

    assert_eq!(tree.len()?, ITEM_COUNT);
    assert!(tree
        .iter()?
        .into_iter()
        .all(|x| x.unwrap().value == "new".as_bytes().to_vec()));

    Ok(())
}

#[test]
fn tree_shadowing_prefix() -> lsm_tree::Result<()> {
    const ITEM_COUNT: usize = 10_000;

    let folder = tempfile::tempdir()?.into_path();

    let tree = Config::new(folder).block_size(1_024).open()?;

    for x in 0..ITEM_COUNT as u64 {
        let value = "old";
        tree.insert(format!("pre:{x}"), value)?;
        tree.insert(format!("prefix:{x}"), value)?;
    }

    tree.wait_for_memtable_flush()?;

    assert_eq!(tree.len()?, ITEM_COUNT * 2);
    assert_eq!(tree.prefix("pre")?.into_iter().count(), ITEM_COUNT * 2);
    assert_eq!(tree.prefix("prefix")?.into_iter().count(), ITEM_COUNT);
    assert!(tree
        .iter()?
        .into_iter()
        .all(|x| x.unwrap().value == "old".as_bytes().to_vec()));

    for x in 0..ITEM_COUNT as u64 {
        let value = "new";
        tree.insert(format!("pre:{x}"), value)?;
        tree.insert(format!("prefix:{x}"), value)?;
    }

    assert_eq!(tree.len()?, ITEM_COUNT * 2);
    assert_eq!(tree.prefix("pre")?.into_iter().count(), ITEM_COUNT * 2);
    assert_eq!(tree.prefix("prefix")?.into_iter().count(), ITEM_COUNT);
    assert!(tree
        .iter()?
        .into_iter()
        .all(|x| x.unwrap().value == "new".as_bytes().to_vec()));

    tree.wait_for_memtable_flush()?;

    assert_eq!(tree.len()?, ITEM_COUNT * 2);
    assert_eq!(tree.prefix("pre")?.into_iter().count(), ITEM_COUNT * 2);
    assert_eq!(tree.prefix("prefix")?.into_iter().count(), ITEM_COUNT);
    assert!(tree
        .iter()?
        .into_iter()
        .all(|x| x.unwrap().value == "new".as_bytes().to_vec()));

    Ok(())
}
