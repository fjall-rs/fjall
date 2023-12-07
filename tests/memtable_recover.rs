use lsm_tree::Config;
use test_log::test;

const ITEM_COUNT: usize = 100_000;

#[test]
fn tree_reload_with_memtable() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    // NOTE: clippy bug
    #[allow(unused_assignments)]
    let mut bytes_before = 0;

    {
        let tree = Config::new(&folder).block_size(1_024).open()?;

        assert_eq!(0, tree.disk_space()?);

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

        tree.flush()?;

        bytes_before = tree.disk_space()?;

        assert!(bytes_before > 0);
        assert_eq!(tree.len()?, ITEM_COUNT * 2);
        assert_eq!(
            tree.iter()?.into_iter().filter(Result::is_ok).count(),
            ITEM_COUNT * 2
        );
        assert_eq!(
            tree.iter()?.into_iter().rev().filter(Result::is_ok).count(),
            ITEM_COUNT * 2
        );
    }

    {
        let tree = Config::new(&folder).block_size(1_024).open()?;

        assert_eq!(bytes_before, tree.disk_space()?);
        assert_eq!(tree.len()?, ITEM_COUNT * 2);
        assert_eq!(
            tree.iter()?.into_iter().filter(Result::is_ok).count(),
            ITEM_COUNT * 2
        );
        assert_eq!(
            tree.iter()?.into_iter().rev().filter(Result::is_ok).count(),
            ITEM_COUNT * 2
        );
    }

    Ok(())
}
