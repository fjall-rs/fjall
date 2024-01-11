use lsm_tree::Config;
use std::sync::Arc;
use tempfile::tempdir;
use test_log::test;

const ITEM_COUNT: usize = 1_000;

#[test]
fn tree_memtable_count() -> lsm_tree::Result<()> {
    let folder = tempdir()?.into_path();

    let tree = Config::new(folder).open()?;

    for x in 0..ITEM_COUNT as u64 {
        let key = x.to_be_bytes();
        let value = nanoid::nanoid!();
        tree.insert(key, value.as_bytes(), 0);
    }

    assert_eq!(tree.len()?, ITEM_COUNT);
    assert_eq!(
        tree.iter().into_iter().filter(|x| x.is_ok()).count(),
        ITEM_COUNT
    );
    assert_eq!(
        tree.iter().into_iter().rev().filter(|x| x.is_ok()).count(),
        ITEM_COUNT
    );

    Ok(())
}

#[test]
fn tree_flushed_count() -> lsm_tree::Result<()> {
    let folder = tempdir()?.into_path();

    let tree = Config::new(folder).open()?;

    for x in 0..ITEM_COUNT as u64 {
        let key = x.to_be_bytes();
        let value = nanoid::nanoid!();
        tree.insert(key, value.as_bytes(), 0);
    }

    tree.flush_active_memtable()?;

    assert_eq!(tree.len()?, ITEM_COUNT);
    assert_eq!(
        tree.iter().into_iter().filter(|x| x.is_ok()).count(),
        ITEM_COUNT
    );
    assert_eq!(
        tree.iter().into_iter().rev().filter(|x| x.is_ok()).count(),
        ITEM_COUNT
    );

    Ok(())
}

#[test]
fn tree_non_locking_count() -> lsm_tree::Result<()> {
    use std::ops::Bound::{self, Excluded, Unbounded};

    let folder = tempdir()?.into_path();

    let tree = Config::new(folder).open()?;

    for x in 0..ITEM_COUNT as u64 {
        let key = x.to_be_bytes();
        let value = "a";
        tree.insert(key, value.as_bytes(), x);
    }

    tree.flush_active_memtable()?;

    // NOTE: don't care
    #[allow(clippy::type_complexity)]
    let mut range: (Bound<Arc<[u8]>>, Bound<Arc<[u8]>>) = (Unbounded, Unbounded);
    let mut count = 0;

    loop {
        let chunk = tree
            .range(range.clone())
            .into_iter()
            .take(10)
            .collect::<lsm_tree::Result<Vec<_>>>()?;

        if chunk.is_empty() {
            break;
        }

        count += chunk.len();

        let (key, _) = chunk.last().unwrap();
        range = (Excluded(key.clone()), Unbounded);
    }

    assert_eq!(count, ITEM_COUNT);

    Ok(())
}
