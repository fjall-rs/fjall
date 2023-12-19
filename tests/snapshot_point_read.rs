use lsm_tree::Config;
use test_log::test;

#[test]
fn snapshot_lots_of_versions() -> lsm_tree::Result<()> {
    let version_count = 100_000;

    let folder = tempfile::tempdir()?;

    let tree = Config::new(&folder).block_size(1_024).open()?;

    let key = "abc";

    for _ in 0u64..version_count {
        tree.insert(key, format!("abc{version_count}").as_bytes())?;
    }

    tree.flush()?;
    tree.wait_for_memtable_flush()?;

    assert_eq!(tree.len()?, 1);

    for seqno in 1..version_count {
        let item = tree
            .get_default_partition()
            .get_internal_entry(key, true, Some(seqno))?
            .expect("should exist");
        assert_eq!(format!("abc{}", version_count).as_bytes(), &*item.value);

        let item = tree.get(key)?.expect("should exist");
        assert_eq!(format!("abc{}", version_count).as_bytes(), &*item);
    }

    Ok(())
}

const ITEM_COUNT: usize = 1;
const BATCHES: usize = 10;

#[test]
fn snapshot_disk_point_reads() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = Config::new(&folder).block_size(1_024).open()?;

    for batch in 0..BATCHES {
        for x in 0..ITEM_COUNT as u64 {
            let key = x.to_be_bytes();
            tree.insert(key, format!("abc{batch}").as_bytes())?;
        }
    }

    tree.flush()?;
    tree.wait_for_memtable_flush()?;

    assert_eq!(tree.len()?, ITEM_COUNT);

    for x in 0..ITEM_COUNT as u64 {
        let key = x.to_be_bytes();

        let item = tree.get(key)?.expect("should exist");
        assert_eq!("abc9".as_bytes(), &*item);
    }

    let snapshot = tree.snapshot();

    assert_eq!(tree.len()?, snapshot.len()?);
    assert_eq!(tree.len()?, snapshot.iter().into_iter().rev().count());

    // This batch will be too new for snapshot (invisible)
    for batch in 0..BATCHES {
        for x in 0..ITEM_COUNT as u64 {
            let key = x.to_be_bytes();
            tree.insert(key, format!("def{batch}").as_bytes())?;
        }
    }
    tree.wait_for_memtable_flush()?;

    for x in 0..ITEM_COUNT as u64 {
        let key = x.to_be_bytes();

        let item = snapshot.get(key)?.expect("should exist");
        assert_eq!("abc9".as_bytes(), &*item);

        let item = tree.get(key)?.expect("should exist");
        assert_eq!("def9".as_bytes(), &*item);
    }

    Ok(())
}

#[test]
fn snapshot_disk_and_memtable_reads() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = Config::new(&folder).block_size(1_024).open()?;

    for batch in 0..BATCHES {
        for x in 0..ITEM_COUNT as u64 {
            let key = x.to_be_bytes();
            tree.insert(key, format!("abc{batch}").as_bytes())?;
        }
    }

    tree.flush()?;
    tree.wait_for_memtable_flush()?;

    assert_eq!(tree.len()?, ITEM_COUNT);

    let snapshot = tree.snapshot();

    assert_eq!(tree.len()?, snapshot.len()?);
    assert_eq!(tree.len()?, snapshot.iter().into_iter().rev().count());

    // This batch will be in memtable and too new for snapshot (invisible)
    for batch in 0..BATCHES {
        for x in 0..ITEM_COUNT as u64 {
            let key = x.to_be_bytes();
            tree.insert(key, format!("def{batch}").as_bytes())?;
        }
    }

    for x in 0..ITEM_COUNT as u64 {
        let key = x.to_be_bytes();

        let item = snapshot.get(key)?.expect("should exist");
        assert_eq!("abc9".as_bytes(), &*item);

        let item = tree.get(key)?.expect("should exist");
        assert_eq!("def9".as_bytes(), &*item);
    }

    Ok(())
}
