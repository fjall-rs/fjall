use lsm_tree::{Config, SequenceNumberCounter};
use test_log::test;

#[test]
fn tree_shadowing_upsert() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?.into_path();

    let tree = Config::new(folder).block_size(1_024).open()?;

    let key = "1".as_bytes();
    let value = "oldvalue".as_bytes();

    assert_eq!(tree.len()?, 0);
    tree.insert(key, value, 0);
    assert_eq!(tree.len()?, 1);
    assert_eq!(tree.get(key)?, Some(value.into()));

    tree.flush_active_memtable()?;
    assert_eq!(tree.len()?, 1);
    assert_eq!(tree.get(key)?, Some(value.into()));

    let value = "newvalue".as_bytes();

    tree.insert(key, value, 1);
    assert_eq!(tree.len()?, 1);
    assert_eq!(tree.get(key)?, Some(value.into()));

    tree.flush_active_memtable()?;
    assert_eq!(tree.len()?, 1);
    assert_eq!(tree.get(key)?, Some(value.into()));

    Ok(())
}

#[test]
fn tree_shadowing_delete() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?.into_path();

    let tree = Config::new(folder).block_size(1_024).open().unwrap();

    let key = "1".as_bytes();
    let value = "oldvalue".as_bytes();

    assert_eq!(tree.len()?, 0);
    tree.insert(key, value, 0);
    assert_eq!(tree.len()?, 1);
    assert_eq!(tree.get(key)?, Some(value.into()));

    tree.flush_active_memtable()?;
    assert_eq!(tree.len()?, 1);
    assert_eq!(tree.get(key)?, Some(value.into()));

    tree.remove(key, 1);
    assert_eq!(tree.len()?, 0);
    assert!(tree.get(key)?.is_none());

    tree.flush_active_memtable()?;
    assert_eq!(tree.len()?, 0);
    assert!(tree.get(key)?.is_none());

    Ok(())
}

#[test]
fn tree_shadowing_range() -> lsm_tree::Result<()> {
    const ITEM_COUNT: usize = 10_000;

    let folder = tempfile::tempdir()?.into_path();

    let tree = Config::new(folder).block_size(1_024).open()?;

    let seqno = SequenceNumberCounter::default();

    for x in 0..ITEM_COUNT as u64 {
        let key = x.to_be_bytes();
        let value = "old".as_bytes();
        tree.insert(key, value, seqno.next());
    }

    tree.flush_active_memtable()?;

    assert_eq!(tree.len()?, ITEM_COUNT);
    assert!(tree
        .iter()
        .into_iter()
        .all(|x| x.unwrap().1 == "old".as_bytes().into()));

    for x in 0..ITEM_COUNT as u64 {
        let key = x.to_be_bytes();
        let value = "new".as_bytes();
        tree.insert(key, value, seqno.next());
    }

    assert_eq!(tree.len()?, ITEM_COUNT);
    assert!(tree
        .iter()
        .into_iter()
        .all(|x| x.unwrap().1 == "new".as_bytes().into()));

    tree.flush_active_memtable()?;

    assert_eq!(tree.len()?, ITEM_COUNT);
    assert!(tree
        .iter()
        .into_iter()
        .all(|x| x.unwrap().1 == "new".as_bytes().into()));

    Ok(())
}

#[test]
fn tree_shadowing_prefix() -> lsm_tree::Result<()> {
    const ITEM_COUNT: usize = 10_000;

    let folder = tempfile::tempdir()?.into_path();

    let tree = Config::new(folder).block_size(1_024).open()?;

    let seqno = SequenceNumberCounter::default();

    for x in 0..ITEM_COUNT as u64 {
        let value = "old".as_bytes();
        let batch_seqno = seqno.next();

        tree.insert(format!("pre:{x}").as_bytes(), value, batch_seqno);
        tree.insert(format!("prefix:{x}").as_bytes(), value, batch_seqno);
    }

    tree.flush_active_memtable()?;

    assert_eq!(tree.len()?, ITEM_COUNT * 2);
    assert_eq!(
        tree.prefix("pre".as_bytes()).into_iter().count(),
        ITEM_COUNT * 2
    );
    assert_eq!(
        tree.prefix("prefix".as_bytes()).into_iter().count(),
        ITEM_COUNT
    );
    assert!(tree
        .iter()
        .into_iter()
        .all(|x| x.unwrap().1 == "old".as_bytes().into()));

    for x in 0..ITEM_COUNT as u64 {
        let value = "new".as_bytes();
        let batch_seqno = seqno.next();

        tree.insert(format!("pre:{x}").as_bytes(), value, batch_seqno);
        tree.insert(format!("prefix:{x}").as_bytes(), value, batch_seqno);
    }

    assert_eq!(tree.len()?, ITEM_COUNT * 2);
    assert_eq!(
        tree.prefix("pre".as_bytes()).into_iter().count(),
        ITEM_COUNT * 2
    );
    assert_eq!(
        tree.prefix("prefix".as_bytes()).into_iter().count(),
        ITEM_COUNT
    );
    assert!(tree
        .iter()
        .into_iter()
        .all(|x| x.unwrap().1 == "new".as_bytes().into()));

    tree.flush_active_memtable()?;

    assert_eq!(tree.len()?, ITEM_COUNT * 2);
    assert_eq!(
        tree.prefix("pre".as_bytes()).into_iter().count(),
        ITEM_COUNT * 2
    );
    assert_eq!(
        tree.prefix("prefix".as_bytes()).into_iter().count(),
        ITEM_COUNT
    );
    assert!(tree
        .iter()
        .into_iter()
        .all(|x| x.unwrap().1 == "new".as_bytes().into()));

    Ok(())
}
