use lsm_tree::{BlockCache, Config};
use std::sync::Arc;
use tempfile::tempdir;
use test_log::test;

#[test]
fn tree_flushed_count() -> lsm_tree::Result<()> {
    let block_cache = Arc::new(BlockCache::with_capacity_blocks(10_000));

    let folder = tempdir()?.into_path();

    let tree1 = Config::new(folder)
        .block_cache(block_cache.clone())
        .open()?;

    let folder = tempfile::tempdir()?;
    let tree2 = Config::new(folder)
        .block_cache(block_cache.clone())
        .open()?;

    tree1.insert("abc", "def")?;
    tree2.insert("def", "abc")?;

    tree1.wait_for_memtable_flush()?;
    tree2.wait_for_memtable_flush()?;

    assert!(tree1.get("abc")?.is_some());
    assert_eq!(tree1.len()?, 1);
    assert_eq!(
        /* index block */ 1 + /* data block */ 1,
        block_cache.len()
    );

    assert!(tree2.get("def")?.is_some());
    assert_eq!(tree2.len()?, 1);
    assert_eq!(
        /* index blocks */ 2 + /* data blocks */ 2,
        block_cache.len()
    );

    assert_eq!("def".as_bytes(), &*tree1.get("abc")?.expect("should exist"));
    assert_eq!("abc".as_bytes(), &*tree2.get("def")?.expect("should exist"));

    assert!(tree1.get("def")?.is_none());
    assert!(tree2.get("abc")?.is_none());

    Ok(())
}
