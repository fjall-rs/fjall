use lsm_tree::Config;
use test_log::test;

#[test]
fn tree_major_compaction() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let path = folder.path();

    let tree = Config::new(path).open()?;

    tree.insert("a".as_bytes(), nanoid::nanoid!().as_bytes())?;
    tree.insert("b".as_bytes(), nanoid::nanoid!().as_bytes())?;
    tree.insert("c".as_bytes(), nanoid::nanoid!().as_bytes())?;
    tree.flush()?;
    tree.wait_for_memtable_flush()?;
    tree.do_major_compaction(u64::MAX)
        .join()
        .expect("should join")?;

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

    assert_eq!(3, tree.len()?);
    assert_eq!(1, tree.get_default_partition().segment_count());

    tree.remove("a".as_bytes())?;
    tree.remove("b".as_bytes())?;
    tree.remove("c".as_bytes())?;
    tree.flush()?;
    tree.wait_for_memtable_flush()?;

    tree.do_major_compaction(u64::MAX)
        .join()
        .expect("should join")?;

    assert_eq!(0, tree.len()?);
    assert_eq!(0, tree.get_default_partition().segment_count());

    Ok(())
}
