use lsm_tree::Config;
use tempfile::tempdir;
use test_log::test;

#[test]
fn tree_range_count() -> lsm_tree::Result<()> {
    let folder = tempdir()?.into_path();

    let tree = Config::new(folder).open()?;

    tree.insert("a".as_bytes(), nanoid::nanoid!().as_bytes())?;
    tree.insert("f".as_bytes(), nanoid::nanoid!().as_bytes())?;
    tree.insert("g".as_bytes(), nanoid::nanoid!().as_bytes())?;
    assert_eq!(2, tree.range("a"..="f").into_iter().count());

    tree.flush()?;
    tree.wait_for_memtable_flush()?;

    tree.insert("a".as_bytes(), nanoid::nanoid!().as_bytes())?;
    tree.insert("f".as_bytes(), nanoid::nanoid!().as_bytes())?;
    tree.insert("g".as_bytes(), nanoid::nanoid!().as_bytes())?;
    assert_eq!(2, tree.range("a"..="f").into_iter().count());

    Ok(())
}
