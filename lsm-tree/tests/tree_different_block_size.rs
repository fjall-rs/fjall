use lsm_tree::{Config, SequenceNumberCounter};
use tempfile::tempdir;
use test_log::test;

const ITEM_COUNT: usize = 1_000;

#[test]
fn tree_block_size_after_recovery() -> lsm_tree::Result<()> {
    let folder = tempdir()?.into_path();

    {
        let tree = Config::new(&folder).block_size(2_048).open()?;

        let seqno = SequenceNumberCounter::default();

        for x in 0..ITEM_COUNT as u64 {
            let key = x.to_be_bytes();
            let value = nanoid::nanoid!();
            tree.insert(key, value.as_bytes(), seqno.next());
        }

        tree.flush_active_memtable()?;

        assert_eq!(ITEM_COUNT, tree.len()?);
    }

    {
        let tree = Config::new(&folder).block_size(2_048).open()?;
        assert_eq!(ITEM_COUNT, tree.len()?);
    }

    {
        let tree = Config::new(&folder).block_size(4_096).open()?;
        assert_eq!(ITEM_COUNT, tree.len()?);
    }

    {
        let tree = Config::new(&folder).block_size(78_652).open()?;
        assert_eq!(ITEM_COUNT, tree.len()?);
    }

    Ok(())
}
