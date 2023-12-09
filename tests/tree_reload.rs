use std::fs::create_dir_all;

use lsm_tree::Config;
use test_log::test;

const ITEM_COUNT: usize = 100_000;

#[test]
fn tree_reload_empty() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    {
        let tree = Config::new(&folder).block_size(1_024).open()?;

        assert_eq!(tree.len()?, 0);
        assert_eq!(tree.iter()?.into_iter().filter(Result::is_ok).count(), 0);
        assert_eq!(
            tree.iter()?.into_iter().rev().filter(Result::is_ok).count(),
            0
        );
    }

    {
        let tree = Config::new(&folder).open()?;

        assert_eq!(tree.len()?, 0);
        assert_eq!(tree.iter()?.into_iter().filter(Result::is_ok).count(), 0);
        assert_eq!(
            tree.iter()?.into_iter().rev().filter(Result::is_ok).count(),
            0
        );
    }

    Ok(())
}

#[test]
fn tree_reload() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    {
        let tree = Config::new(&folder).block_size(1_024).open()?;

        for x in 0..ITEM_COUNT as u64 {
            let key = x.to_be_bytes();
            let value = nanoid::nanoid!();
            tree.insert(key, value.as_bytes())?;
        }

        tree.flush()?;
        tree.wait_for_memtable_flush()?;

        for x in 0..ITEM_COUNT as u64 {
            let key: [u8; 8] = (x + ITEM_COUNT as u64).to_be_bytes();
            let value = nanoid::nanoid!();
            tree.insert(key, value.as_bytes())?;
        }

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
        let tree = Config::new(&folder).fsync_ms(None).open()?;

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

    std::thread::sleep(std::time::Duration::from_secs(2));

    Ok(())
}

#[test]
fn tree_remove_unfinished_segments() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let path = folder.path();

    let subfolder = path.join("segments").join("abc");
    create_dir_all(&subfolder)?;
    assert!(subfolder.exists());

    // Setup tree
    {
        let tree = Config::new(&folder).block_size(1_024).open()?;

        assert_eq!(tree.len()?, 0);
        assert_eq!(tree.iter()?.into_iter().filter(Result::is_ok).count(), 0);
        assert_eq!(
            tree.iter()?.into_iter().rev().filter(Result::is_ok).count(),
            0
        );
    }

    // Recover tree
    {
        let tree = Config::new(&folder).block_size(1_024).open()?;

        assert_eq!(tree.len()?, 0);
        assert_eq!(tree.iter()?.into_iter().filter(Result::is_ok).count(), 0);
        assert_eq!(
            tree.iter()?.into_iter().rev().filter(Result::is_ok).count(),
            0
        );
    }

    assert!(!subfolder.exists());

    Ok(())
}
