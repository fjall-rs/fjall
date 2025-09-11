use fjall::{Database, KeyspaceCreateOptions};
use lsm_tree::Guard;
use test_log::test;

const ITEM_COUNT: usize = 100;

#[test]
fn recover_seqno() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    let mut seqno = 0;

    // NOTE: clippy bug
    #[allow(unused_assignments)]
    {
        let db = Database::builder(&folder).open()?;

        let keyspaces = &[
            db.keyspace("default1", KeyspaceCreateOptions::default())?,
            db.keyspace("default2", KeyspaceCreateOptions::default())?,
            db.keyspace("default3", KeyspaceCreateOptions::default())?,
        ];

        for tree in keyspaces {
            for x in 0..ITEM_COUNT as u64 {
                let key = x.to_be_bytes();
                let value = nanoid::nanoid!();
                tree.insert(key, value.as_bytes())?;

                seqno += 1;
                assert_eq!(seqno, db.seqno());
            }

            for x in 0..ITEM_COUNT as u64 {
                let key: [u8; 8] = (x + ITEM_COUNT as u64).to_be_bytes();
                let value = nanoid::nanoid!();
                tree.insert(key, value.as_bytes())?;

                seqno += 1;
                assert_eq!(seqno, db.seqno());
            }
        }

        for tree in keyspaces {
            assert_eq!(tree.len()?, ITEM_COUNT * 2);
            assert_eq!(tree.iter().flat_map(|x| x.key()).count(), ITEM_COUNT * 2);
            assert_eq!(
                tree.iter().rev().flat_map(|x| x.key()).count(),
                ITEM_COUNT * 2
            );
        }
    }

    for _ in 0..10 {
        let db = Database::builder(&folder).open()?;
        assert_eq!(seqno, db.seqno());

        let keyspaces = &[
            db.keyspace("default1", KeyspaceCreateOptions::default())?,
            db.keyspace("default2", KeyspaceCreateOptions::default())?,
            db.keyspace("default3", KeyspaceCreateOptions::default())?,
        ];

        for tree in keyspaces {
            assert_eq!(tree.len()?, ITEM_COUNT * 2);
            assert_eq!(tree.iter().flat_map(|x| x.key()).count(), ITEM_COUNT * 2);
            assert_eq!(
                tree.iter().rev().flat_map(|x| x.key()).count(),
                ITEM_COUNT * 2
            );
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
        let db = Database::builder(&folder).open()?;

        let keyspaces = &[
            db.keyspace("default1", KeyspaceCreateOptions::default())?,
            db.keyspace("default2", KeyspaceCreateOptions::default())?,
            db.keyspace("default3", KeyspaceCreateOptions::default())?,
        ];

        for tree in keyspaces {
            for x in 0..ITEM_COUNT as u64 {
                let key = x.to_be_bytes();
                tree.remove(key)?;

                seqno += 1;
                assert_eq!(seqno, db.seqno());
            }

            for x in 0..ITEM_COUNT as u64 {
                let key: [u8; 8] = (x + ITEM_COUNT as u64).to_be_bytes();
                tree.remove(key)?;

                seqno += 1;
                assert_eq!(seqno, db.seqno());
            }
        }
    }

    for _ in 0..10 {
        let db = Database::builder(&folder).open()?;
        assert_eq!(seqno, db.seqno());
    }

    Ok(())
}
