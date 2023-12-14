use lsm_tree::Config;
use tempfile::tempdir;
use test_log::test;

#[test]
fn tree_range_count() -> lsm_tree::Result<()> {
    use std::ops::Bound::{self, Excluded, Unbounded};

    let folder = tempdir()?.into_path();

    let tree = Config::new(folder).open()?;

    tree.insert("a".as_bytes(), nanoid::nanoid!().as_bytes())?;
    tree.insert("f".as_bytes(), nanoid::nanoid!().as_bytes())?;
    tree.insert("g".as_bytes(), nanoid::nanoid!().as_bytes())?;

    tree.insert("a".as_bytes(), nanoid::nanoid!().as_bytes())?;
    tree.insert("f".as_bytes(), nanoid::nanoid!().as_bytes())?;
    tree.insert("g".as_bytes(), nanoid::nanoid!().as_bytes())?;

    assert_eq!(2, tree.range("a"..="f").into_iter().count());
    assert_eq!(2, tree.range("f"..="g").into_iter().count());

    assert_eq!(
        1,
        tree.range::<Vec<u8>, (Bound<Vec<u8>>, Bound<Vec<u8>>)>((Excluded("f".into()), Unbounded))
            .into_iter()
            .count()
    );

    tree.flush()?;
    tree.wait_for_memtable_flush()?;

    assert_eq!(2, tree.range("a"..="f").into_iter().count());
    assert_eq!(
        1,
        tree.range::<Vec<u8>, (Bound<Vec<u8>>, Bound<Vec<u8>>)>((Excluded("f".into()), Unbounded))
            .into_iter()
            .count()
    );

    tree.insert("a".as_bytes(), nanoid::nanoid!().as_bytes())?;
    tree.insert("f".as_bytes(), nanoid::nanoid!().as_bytes())?;
    tree.insert("g".as_bytes(), nanoid::nanoid!().as_bytes())?;

    assert_eq!(2, tree.range("a"..="f").into_iter().count());
    assert_eq!(
        1,
        tree.range::<Vec<u8>, (Bound<Vec<u8>>, Bound<Vec<u8>>)>((Excluded("f".into()), Unbounded))
            .into_iter()
            .count()
    );

    Ok(())
}
