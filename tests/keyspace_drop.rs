#[cfg(feature = "__internal_integration")]
#[test_log::test]
fn integration_keyspace_drop() -> fjall::Result<()> {
    use fjall::Config;

    {
        let folder = tempfile::tempdir()?;

        assert_eq!(0, fjall::drop::load_drop_counter());
        let keyspace = Config::new(folder).open()?;
        assert_eq!(5, fjall::drop::load_drop_counter());

        drop(keyspace);
        assert_eq!(0, fjall::drop::load_drop_counter());
    }

    {
        let folder = tempfile::tempdir()?;

        assert_eq!(0, fjall::drop::load_drop_counter());
        let keyspace = Config::new(folder).open()?;
        assert_eq!(5, fjall::drop::load_drop_counter());

        let partition = keyspace.open_partition("default", Default::default())?;
        assert_eq!(6, fjall::drop::load_drop_counter());

        drop(partition);
        drop(keyspace);
        assert_eq!(0, fjall::drop::load_drop_counter());
    }

    {
        let folder = tempfile::tempdir()?;

        assert_eq!(0, fjall::drop::load_drop_counter());
        let keyspace = Config::new(folder).open()?;
        assert_eq!(5, fjall::drop::load_drop_counter());

        let _partition = keyspace.open_partition("default", Default::default())?;
        assert_eq!(6, fjall::drop::load_drop_counter());

        let _partition2 = keyspace.open_partition("different", Default::default())?;
        assert_eq!(7, fjall::drop::load_drop_counter());
    }

    assert_eq!(0, fjall::drop::load_drop_counter());

    Ok(())
}

#[cfg(feature = "__internal_integration")]
#[test_log::test]
fn integration_keyspace_drop_2() -> fjall::Result<()> {
    use fjall::{Config, PartitionCreateOptions};

    const ITEM_COUNT: usize = 10;

    let folder = tempfile::tempdir()?;

    {
        let keyspace = Config::new(&folder).open()?;

        let partitions = &[
            keyspace.open_partition("tree1", PartitionCreateOptions::default())?,
            keyspace.open_partition("tree2", PartitionCreateOptions::default())?,
            keyspace.open_partition("tree3", PartitionCreateOptions::default())?,
        ];

        for tree in partitions {
            for x in 0..ITEM_COUNT as u64 {
                let key = x.to_be_bytes();
                let value = nanoid::nanoid!();
                tree.insert(key, value.as_bytes())?;
            }
        }

        for tree in partitions {
            assert_eq!(tree.len()?, ITEM_COUNT);
            assert_eq!(tree.iter().flatten().count(), ITEM_COUNT);
            assert_eq!(tree.iter().rev().flatten().count(), ITEM_COUNT);
        }

        partitions.first().unwrap().rotate_memtable_and_wait()?;

        for tree in partitions {
            for x in 0..ITEM_COUNT as u64 {
                let key: [u8; 8] = (x + ITEM_COUNT as u64).to_be_bytes();
                let value = nanoid::nanoid!();
                tree.insert(key, value.as_bytes())?;
            }
        }

        for tree in partitions {
            assert_eq!(tree.len()?, ITEM_COUNT * 2);
            assert_eq!(tree.iter().flatten().count(), ITEM_COUNT * 2);
            assert_eq!(tree.iter().rev().flatten().count(), ITEM_COUNT * 2);
        }
    }

    assert_eq!(0, fjall::drop::load_drop_counter());

    Ok(())
}
