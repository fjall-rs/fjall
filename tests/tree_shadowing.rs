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
