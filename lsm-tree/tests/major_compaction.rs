use lsm_tree::{Config, SequenceNumberCounter};
use test_log::test;

#[test]
fn tree_major_compaction() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let path = folder.path();

    let tree = Config::new(path).open()?;

    let seqno = SequenceNumberCounter::default();

    tree.insert("a".as_bytes(), "abc", seqno.next());
    tree.insert("b".as_bytes(), "abc", seqno.next());
    tree.insert("c".as_bytes(), "abc", seqno.next());

    tree.flush_active_memtable()?;
    tree.major_compact(u64::MAX)?;

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
    assert_eq!(1, tree.segment_count());

    let batch_seqno = seqno.next();
    tree.remove("a".as_bytes(), batch_seqno);
    tree.remove("b".as_bytes(), batch_seqno);
    tree.remove("c".as_bytes(), batch_seqno);

    tree.flush_active_memtable()?;
    tree.major_compact(u64::MAX)?;

    assert_eq!(0, tree.len()?);
    assert_eq!(0, tree.segment_count());

    Ok(())
}
