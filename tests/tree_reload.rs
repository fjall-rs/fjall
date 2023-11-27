use lsm_tree::Config;
use test_log::test;

const ITEM_COUNT: usize = 1_000;

#[test]
fn tree_reload() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    {
        let db = Config::new(&folder).open()?;

        for x in 0..ITEM_COUNT as u64 {
            let key = x.to_be_bytes();
            let value = nanoid::nanoid!();
            db.insert(key, value)?;
        }

        db.wait_for_memtable_flush()?;

        for x in 0..ITEM_COUNT as u64 {
            let key: [u8; 8] = (x + ITEM_COUNT as u64).to_be_bytes();
            let value = nanoid::nanoid!();
            db.insert(key, value)?;
        }

        assert_eq!(db.len()?, ITEM_COUNT * 2);
        assert_eq!(
            db.iter()?.into_iter().filter(Result::is_ok).count(),
            ITEM_COUNT * 2
        );
        assert_eq!(
            db.iter()?.into_iter().rev().filter(Result::is_ok).count(),
            ITEM_COUNT * 2
        );
    }

    {
        let db = Config::new(&folder).open()?;

        assert_eq!(db.len()?, ITEM_COUNT * 2);
        assert_eq!(
            db.iter()?.into_iter().filter(Result::is_ok).count(),
            ITEM_COUNT * 2
        );
        assert_eq!(
            db.iter()?.into_iter().rev().filter(Result::is_ok).count(),
            ITEM_COUNT * 2
        );
    }

    Ok(())
}
