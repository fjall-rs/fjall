use fjall::{Config, PartitionConfig};
use test_log::test;

const ITEM_COUNT: usize = 10_000;

#[test]
fn tree_reload_with_memtable() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    // NOTE: clippy bug
    #[allow(unused_assignments)]
    {
        let keyspace = Config::new(&folder).open()?;
        let tree = keyspace.open_partition("default", PartitionConfig::default())?;

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

        keyspace.persist()?;

        assert_eq!(tree.len()?, ITEM_COUNT * 2);
        assert_eq!(
            tree.iter().into_iter().filter(Result::is_ok).count(),
            ITEM_COUNT * 2
        );
        assert_eq!(
            tree.iter().into_iter().rev().filter(Result::is_ok).count(),
            ITEM_COUNT * 2
        );
    }

    {
        let keyspace = Config::new(&folder).open()?;
        let tree = keyspace.open_partition("default", PartitionConfig::default())?;

        assert_eq!(tree.len()?, ITEM_COUNT * 2);
        assert_eq!(
            tree.iter().into_iter().filter(Result::is_ok).count(),
            ITEM_COUNT * 2
        );
        assert_eq!(
            tree.iter().into_iter().rev().filter(Result::is_ok).count(),
            ITEM_COUNT * 2
        );
    }

    Ok(())
}
