use lsm_tree::Config;
use test_log::test;

#[test]
fn tree_delete_by_prefix() -> lsm_tree::Result<()> {
    const ITEM_COUNT: usize = 10_000;

    let folder = tempfile::tempdir()?.into_path();

    let tree = Config::new(folder).block_size(1_024).open()?;

    for x in 0..ITEM_COUNT as u64 {
        let value = "old".as_bytes();
        tree.insert(format!("a:{x}").as_bytes(), value)?;
        tree.insert(format!("b:{x}").as_bytes(), value)?;
        tree.insert(format!("c:{x}").as_bytes(), value)?;
    }

    tree.wait_for_memtable_flush()?;

    assert_eq!(tree.len()?, ITEM_COUNT * 3);
    assert_eq!(
        tree.prefix("a:".as_bytes())?.into_iter().count(),
        ITEM_COUNT
    );
    assert_eq!(
        tree.prefix("b:".as_bytes())?.into_iter().count(),
        ITEM_COUNT
    );
    assert_eq!(
        tree.prefix("c:".as_bytes())?.into_iter().count(),
        ITEM_COUNT
    );

    for item in &tree.prefix("b:".as_bytes())? {
        let (key, _) = item?;
        tree.remove(key)?;
    }

    assert_eq!(tree.len()?, ITEM_COUNT * 2);
    assert_eq!(
        tree.prefix("a:".as_bytes())?.into_iter().count(),
        ITEM_COUNT
    );
    assert_eq!(tree.prefix("b:".as_bytes())?.into_iter().count(), 0);
    assert_eq!(
        tree.prefix("c:".as_bytes())?.into_iter().count(),
        ITEM_COUNT
    );

    Ok(())
}

#[test]
fn tree_delete_by_range() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?.into_path();

    let tree = Config::new(folder).block_size(1_024).open()?;

    let value = "old".as_bytes();
    tree.insert("a".as_bytes(), value)?;
    tree.insert("b".as_bytes(), value)?;
    tree.insert("c".as_bytes(), value)?;
    tree.insert("d".as_bytes(), value)?;
    tree.insert("e".as_bytes(), value)?;
    tree.insert("f".as_bytes(), value)?;

    tree.wait_for_memtable_flush()?;

    assert_eq!(tree.len()?, 6);

    for item in &tree.range("c"..="e")? {
        let (key, _) = item?;
        tree.remove(key)?;
    }

    assert_eq!(tree.len()?, 3);

    Ok(())
}
