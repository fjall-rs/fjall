use lsm_tree::Config;
use test_log::test;

#[test]
fn tree_shadowing_upsert() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?.into_path();

    let db = Config::new(folder).open()?;

    let key = "1";
    let value = b"oldvalue";

    assert_eq!(db.len()?, 0);
    db.insert(key, *value)?;
    assert_eq!(db.len()?, 1);
    assert_eq!(db.get(key)?.map(|x| x.value), Some(value.to_vec()));

    db.wait_for_memtable_flush()?;
    assert_eq!(db.len()?, 1);
    assert_eq!(db.get(key)?.map(|x| x.value), Some(value.to_vec()));

    let value = b"newvalue";

    db.insert(key, *value)?;
    assert_eq!(db.len()?, 1);
    assert_eq!(db.get(key)?.map(|x| x.value), Some(value.to_vec()));

    db.wait_for_memtable_flush()?;
    assert_eq!(db.len()?, 1);
    assert_eq!(db.get(key)?.map(|x| x.value), Some(value.to_vec()));

    Ok(())
}

#[test]
fn tree_shadowing_delete() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?.into_path();

    let db = Config::new(folder).open().unwrap();

    let key = "1";
    let value = b"oldvalue";

    assert_eq!(db.len()?, 0);
    db.insert(key, *value)?;
    assert_eq!(db.len()?, 1);
    assert_eq!(db.get(key)?.map(|x| x.value), Some(value.to_vec()));

    db.wait_for_memtable_flush()?;
    assert_eq!(db.len()?, 1);
    assert_eq!(db.get(key)?.map(|x| x.value), Some(value.to_vec()));

    db.remove(key)?;
    eprint!("removed key {:?}", key);
    assert_eq!(db.len()?, 0);
    assert!(db.get(key)?.is_none());

    db.wait_for_memtable_flush()?;
    assert_eq!(db.len()?, 0);
    assert!(db.get(key)?.is_none());

    Ok(())
}
