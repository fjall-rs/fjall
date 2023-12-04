use lsm_tree::Config;
use tempfile::tempdir;
use test_log::test;

#[test]
fn tree_range_count() -> lsm_tree::Result<()> {
    let folder = tempdir()?.into_path();

    let tree = Config::new(folder).open()?;

    tree.insert("a", nanoid::nanoid!())?;
    tree.insert("f", nanoid::nanoid!())?;
    tree.insert("g", nanoid::nanoid!())?;
    assert_eq!(2, tree.range("a"..="f")?.into_iter().count());

    tree.flush()?;
    tree.wait_for_memtable_flush()?;

    tree.insert("a", nanoid::nanoid!())?;
    tree.insert("f", nanoid::nanoid!())?;
    tree.insert("g", nanoid::nanoid!())?;
    assert_eq!(2, tree.range("a"..="f")?.into_iter().count());

    Ok(())
}
