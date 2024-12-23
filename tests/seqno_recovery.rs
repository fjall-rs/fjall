use fjall::{Config, PartitionCreateOptions};
use test_log::test;

const ITEM_COUNT: usize = 100;

#[test]
fn recover_seqno() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    let mut seqno = 0;

    // NOTE: clippy bug
    #[allow(unused_assignments)]
    {
        let keyspace = Config::new(&folder).open()?;

        let partitions = &[
            keyspace.open_partition("default1", PartitionCreateOptions::default())?,
            keyspace.open_partition("default2", PartitionCreateOptions::default())?,
            keyspace.open_partition("default3", PartitionCreateOptions::default())?,
        ];

        for tree in partitions {
            for x in 0..ITEM_COUNT as u64 {
                let key = x.to_be_bytes();
                let value = nanoid::nanoid!();
                tree.insert(key, value.as_bytes())?;

                seqno += 1;
                assert_eq!(seqno, keyspace.instant());
            }

            for x in 0..ITEM_COUNT as u64 {
                let key: [u8; 8] = (x + ITEM_COUNT as u64).to_be_bytes();
                let value = nanoid::nanoid!();
                tree.insert(key, value.as_bytes())?;

                seqno += 1;
                assert_eq!(seqno, keyspace.instant());
            }
        }

        for tree in partitions {
            assert_eq!(tree.len()?, ITEM_COUNT * 2);
            assert_eq!(tree.iter().flatten().count(), ITEM_COUNT * 2);
            assert_eq!(tree.iter().rev().flatten().count(), ITEM_COUNT * 2);
        }
    }

    for _ in 0..10 {
        let keyspace = Config::new(&folder).open()?;
        assert_eq!(seqno, keyspace.instant());

        let partitions = &[
            keyspace.open_partition("default1", PartitionCreateOptions::default())?,
            keyspace.open_partition("default2", PartitionCreateOptions::default())?,
            keyspace.open_partition("default3", PartitionCreateOptions::default())?,
        ];

        for tree in partitions {
            assert_eq!(tree.len()?, ITEM_COUNT * 2);
            assert_eq!(tree.iter().flatten().count(), ITEM_COUNT * 2);
            assert_eq!(tree.iter().rev().flatten().count(), ITEM_COUNT * 2);
        }
    }

    Ok(())
}

#[test]
fn recover_seqno_tombstone() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    let mut seqno = 0;

    // NOTE: clippy bug
    #[allow(unused_assignments)]
    {
        let keyspace = Config::new(&folder).open()?;

        let partitions = &[
            keyspace.open_partition("default1", PartitionCreateOptions::default())?,
            keyspace.open_partition("default2", PartitionCreateOptions::default())?,
            keyspace.open_partition("default3", PartitionCreateOptions::default())?,
        ];

        for tree in partitions {
            for x in 0..ITEM_COUNT as u64 {
                let key = x.to_be_bytes();
                tree.remove(key)?;

                seqno += 1;
                assert_eq!(seqno, keyspace.instant());
            }

            for x in 0..ITEM_COUNT as u64 {
                let key: [u8; 8] = (x + ITEM_COUNT as u64).to_be_bytes();
                tree.remove(key)?;

                seqno += 1;
                assert_eq!(seqno, keyspace.instant());
            }
        }
    }

    for _ in 0..10 {
        let keyspace = Config::new(&folder).open()?;
        assert_eq!(seqno, keyspace.instant());
    }

    Ok(())
}
