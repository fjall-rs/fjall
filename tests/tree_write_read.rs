use lsm_tree::Config;
use test_log::test;

#[test]
fn tree_write_and_read() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?.into_path();

    let tree = Config::new(folder.clone()).open()?;

    tree.insert("a", nanoid::nanoid!())?;
    tree.insert("b", nanoid::nanoid!())?;
    tree.insert("c", nanoid::nanoid!())?;
    tree.flush()?;

    let item = tree.get("a")?.unwrap();
    assert_eq!(item.key, b"a");
    assert!(!item.is_tombstone);
    assert_eq!(item.seqno, 0);

    let item = tree.get("b")?.unwrap();
    assert_eq!(item.key, b"b");
    assert!(!item.is_tombstone);
    assert_eq!(item.seqno, 1);

    let item = tree.get("c")?.unwrap();
    assert_eq!(item.key, b"c");
    assert!(!item.is_tombstone);
    assert_eq!(item.seqno, 2);

    let tree = Config::new(folder).open()?;

    let item = tree.get("a")?.unwrap();
    assert_eq!(item.key, b"a");
    assert!(!item.is_tombstone);
    assert_eq!(item.seqno, 0);

    let item = tree.get("b")?.unwrap();
    assert_eq!(item.key, b"b");
    assert!(!item.is_tombstone);
    assert_eq!(item.seqno, 1);

    let item = tree.get("c")?.unwrap();
    assert_eq!(item.key, b"c");
    assert!(!item.is_tombstone);
    assert_eq!(item.seqno, 2);

    Ok(())
}
