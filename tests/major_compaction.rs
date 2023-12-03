use lsm_tree::Config;
use test_log::test;

#[test]
fn tree_major_compaction() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?.into_path();

    let tree = Config::new(folder.clone()).open()?;

    tree.insert("a", nanoid::nanoid!())?;
    tree.insert("b", nanoid::nanoid!())?;
    tree.insert("c", nanoid::nanoid!())?;
    tree.flush()?;
    tree.wait_for_memtable_flush()?;
    tree.do_major_compaction().join().expect("should join")?;

    let item = tree.get_internal_entry("a", true)?.unwrap();
    assert_eq!(item.key, b"a");
    assert!(!item.is_tombstone);
    assert_eq!(item.seqno, 0);

    let item = tree.get_internal_entry("b", true)?.unwrap();
    assert_eq!(item.key, b"b");
    assert!(!item.is_tombstone);
    assert_eq!(item.seqno, 1);

    let item = tree.get_internal_entry("c", true)?.unwrap();
    assert_eq!(item.key, b"c");
    assert!(!item.is_tombstone);
    assert_eq!(item.seqno, 2);

    assert_eq!(3, tree.len()?);
    assert_eq!(1, tree.segment_count());

    tree.remove("a")?;
    tree.remove("b")?;
    tree.remove("c")?;
    tree.flush()?;
    tree.wait_for_memtable_flush()?;

    tree.do_major_compaction().join().expect("should join")?;

    assert_eq!(0, tree.len()?);
    assert_eq!(0, tree.segment_count());

    Ok(())
}
