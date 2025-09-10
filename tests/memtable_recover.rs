use fjall::{Database, KeyspaceCreateOptions};
use lsm_tree::Guard;
use test_log::test;

const ITEM_COUNT: usize = 10_000;

#[test]
fn reload_with_memtable() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    // NOTE: clippy bug
    #[allow(unused_assignments)]
    {
        let db = Database::builder(&folder).open()?;
        let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;

        for x in 0..ITEM_COUNT as u64 {
            let key = x.to_be_bytes();
            let value = nanoid::nanoid!();
            tree.insert(key, value.as_bytes())?;
        }

        for x in 0..ITEM_COUNT as u64 {
            let key: [u8; 8] = (x + ITEM_COUNT as u64).to_be_bytes();
            let value = nanoid::nanoid!();
            tree.insert(key, value.as_bytes())?;
        }

        assert_eq!(tree.len()?, ITEM_COUNT * 2);
        assert_eq!(tree.iter().flat_map(|x| x.key()).count(), ITEM_COUNT * 2);
        assert_eq!(
            tree.iter().rev().flat_map(|x| x.key()).count(),
            ITEM_COUNT * 2
        );
    }

    for _ in 0..5 {
        let db = Database::builder(&folder).open()?;
        let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;

        assert_eq!(tree.len()?, ITEM_COUNT * 2);
        assert_eq!(tree.iter().flat_map(|x| x.key()).count(), ITEM_COUNT * 2);
        assert_eq!(
            tree.iter().rev().flat_map(|x| x.key()).count(),
            ITEM_COUNT * 2
        );
    }

    Ok(())
}
