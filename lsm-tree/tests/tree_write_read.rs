use lsm_tree::Config;
use test_log::test;

#[test]
fn tree_write_and_read() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?.into_path();

    let tree = Config::new(folder.clone()).open()?;

    tree.insert("a".as_bytes(), nanoid::nanoid!().as_bytes(), 0);
    tree.insert("b".as_bytes(), nanoid::nanoid!().as_bytes(), 1);
    tree.insert("c".as_bytes(), nanoid::nanoid!().as_bytes(), 2);

    let item = tree.get_internal_entry("a", true, None)?.unwrap();
    assert_eq!(item.key, "a".as_bytes().into());
    assert!(!item.is_tombstone());
    assert_eq!(item.seqno, 0);

    let item = tree.get_internal_entry("b", true, None)?.unwrap();
    assert_eq!(item.key, "b".as_bytes().into());
    assert!(!item.is_tombstone());
    assert_eq!(item.seqno, 1);

    let item = tree.get_internal_entry("c", true, None)?.unwrap();
    assert_eq!(item.key, "c".as_bytes().into());
    assert!(!item.is_tombstone());
    assert_eq!(item.seqno, 2);

    tree.flush_active_memtable()?;

    let tree = Config::new(folder).open()?;

    let item = tree.get_internal_entry("a", true, None)?.unwrap();
    assert_eq!(item.key, "a".as_bytes().into());
    assert!(!item.is_tombstone());
    assert_eq!(item.seqno, 0);

    let item = tree.get_internal_entry("b", true, None)?.unwrap();
    assert_eq!(item.key, "b".as_bytes().into());
    assert!(!item.is_tombstone());
    assert_eq!(item.seqno, 1);

    let item = tree.get_internal_entry("c", true, None)?.unwrap();
    assert_eq!(item.key, "c".as_bytes().into());
    assert!(!item.is_tombstone());
    assert_eq!(item.seqno, 2);

    Ok(())
}
