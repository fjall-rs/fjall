use crate::{Database, KeyspaceCreateOptions, KvSeparationOptions};
use test_log::test;

#[test]
#[expect(clippy::unwrap_used)]
fn recover_sealed() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;

    for i in 0_u128..25 {
        let db = Database::create_or_recover(Database::builder(folder.path()).into_config())?;

        let tree = db.keyspace("default", || {
            KeyspaceCreateOptions::default().max_memtable_size(1_000)
        })?;

        assert_eq!(i, tree.len()?.try_into().unwrap());

        tree.insert(i.to_be_bytes(), i.to_be_bytes())?;
        assert_eq!(i + 1, tree.len()?.try_into().unwrap());

        tree.rotate_memtable_and_wait()?;
    }

    Ok(())
}

#[test]
#[expect(clippy::unwrap_used)]
#[ignore = "fails in CI?"]
fn recover_sealed_blob() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;

    for i in 0_u128..25 {
        let db = Database::create_or_recover(Database::builder(folder.path()).into_config())?;

        let tree = db.keyspace("default", || {
            KeyspaceCreateOptions::default()
                .max_memtable_size(1_000)
                .with_kv_separation(Some(KvSeparationOptions::default()))
        })?;

        assert_eq!(i, tree.len()?.try_into().unwrap());

        tree.insert(i.to_be_bytes(), i.to_be_bytes().repeat(1_024))?;
        assert_eq!(i + 1, tree.len()?.try_into().unwrap());

        tree.rotate_memtable_and_wait()?;
    }

    Ok(())
}

#[test]
#[expect(clippy::unwrap_used)]
#[ignore = "fails in CI?"]
fn recover_sealed_pair_1() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;

    for i in 0_u128..25 {
        let db = Database::create_or_recover(Database::builder(folder.path()).into_config())?;

        let tree = db.keyspace("default", || {
            KeyspaceCreateOptions::default().max_memtable_size(1_000)
        })?;
        let tree2 = db.keyspace("default2", || {
            KeyspaceCreateOptions::default()
                .max_memtable_size(1_000)
                .with_kv_separation(Some(KvSeparationOptions::default()))
        })?;

        assert_eq!(i, tree.len()?.try_into().unwrap());
        assert_eq!(i, tree2.len()?.try_into().unwrap());

        let mut batch = db.batch();
        batch.insert(&tree, i.to_be_bytes(), i.to_be_bytes());
        batch.insert(&tree2, i.to_be_bytes(), i.to_be_bytes().repeat(1_024));
        batch.commit()?;

        assert_eq!(i + 1, tree.len()?.try_into().unwrap());
        assert_eq!(i + 1, tree2.len()?.try_into().unwrap());

        tree.rotate_memtable_and_wait()?;
    }

    Ok(())
}
