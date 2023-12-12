use lsm_tree::Config;
use test_log::test;

const ITEM_COUNT: usize = 100;

#[test]
fn snapshot_basic() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = Config::new(&folder).block_size(1_024).open()?;

    for x in 0..ITEM_COUNT as u64 {
        let key = x.to_be_bytes();
        tree.insert(key, "abc".as_bytes())?;
    }

    assert_eq!(tree.len()?, ITEM_COUNT);

    let snapshot = tree.snapshot();

    assert_eq!(tree.len()?, snapshot.len()?);
    assert_eq!(tree.len()?, snapshot.iter().into_iter().rev().count());

    for x in 0..ITEM_COUNT as u64 {
        let key = x.to_be_bytes();
        tree.insert(key, "abc".as_bytes())?;
    }

    tree.flush()?;
    tree.wait_for_memtable_flush()?;
    tree.do_major_compaction().join().expect("should join")?;

    assert_eq!(tree.len()?, ITEM_COUNT);
    assert_eq!(ITEM_COUNT, snapshot.len()?);
    assert_eq!(ITEM_COUNT, snapshot.iter().into_iter().rev().count());

    Ok(())
}
