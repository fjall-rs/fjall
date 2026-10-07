use crate::{Database, KeyspaceCreateOptions, KvSeparationOptions};
use test_log::test;

/// Flushing all keyspaces removes the covered journal and allows reopening without replay.
#[test]
fn flush_all_materializes_journal() -> crate::Result<()> {
    use crate::AbstractTree;

    let folder = tempfile::tempdir()?;

    {
        let db = Database::builder(&folder)
            .worker_threads_unchecked(0)
            .open()?;
        let first = db.keyspace("first", KeyspaceCreateOptions::default)?;
        let second = db.keyspace("second", KeyspaceCreateOptions::default)?;

        let mut batch = db.batch();
        batch.insert(&first, "a", "1");
        batch.insert(&second, "b", "2");
        batch.commit()?;
        first.rotate_memtable()?;
        first.insert("c", "3")?;

        let old_journal = db.supervisor.journal.path()?;
        assert!(old_journal.metadata()?.len() > 0);

        db.flush_all()?;

        let active_journal = db.supervisor.journal.path()?;
        assert_ne!(old_journal, active_journal);
        assert!(!old_journal.exists());
        assert_eq!(0, db.supervisor.journal.get_writer()?.pos()?);
        assert!(first.tree.active_memtable().is_empty());
        assert!(second.tree.active_memtable().is_empty());
        assert_eq!(0, first.tree.sealed_memtable_count());
        assert_eq!(0, second.tree.sealed_memtable_count());

        first.insert("d", "4")?;
        db.flush_all()?;
    }

    {
        let db = Database::builder(&folder)
            .worker_threads_unchecked(0)
            .open()?;
        let first = db.keyspace("first", KeyspaceCreateOptions::default)?;
        let second = db.keyspace("second", KeyspaceCreateOptions::default)?;

        assert_eq!(Some(b"1".as_slice()), first.get("a")?.as_deref());
        assert_eq!(Some(b"2".as_slice()), second.get("b")?.as_deref());
        assert_eq!(Some(b"3".as_slice()), first.get("c")?.as_deref());
        assert_eq!(Some(b"4".as_slice()), first.get("d")?.as_deref());
        assert!(first.tree.active_memtable().is_empty());
        assert!(second.tree.active_memtable().is_empty());
    }

    Ok(())
}

#[test_log::test]
fn clear_recover_sealed() -> crate::Result<()> {
    use crate::{Database, KeyspaceCreateOptions};

    let folder = tempfile::tempdir()?;

    {
        let db = Database::builder(&folder).open()?;

        let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
        assert!(tree.is_empty()?);

        tree.insert("a", "a")?;
        assert!(tree.contains_key("a")?);

        tree.clear()?;
        assert!(tree.is_empty()?);

        tree.rotate_memtable_and_wait()?;
        assert!(tree.is_empty()?);
        db.supervisor.journal.get_writer()?.rotate()?;

        tree.insert("b", "a")?;
        assert!(tree.contains_key("b")?);
    }

    {
        let db = Database::builder(&folder).open()?;

        let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;

        assert!(!tree.contains_key("a")?);
        assert!(tree.contains_key("b")?);
    }

    Ok(())
}

// TODO: investigate: flaky on macOS???
#[cfg(feature = "__internal_whitebox")]
#[test]
#[ignore = "restore"]
fn whitebox_db_drop() -> crate::Result<()> {
    use crate::Database;

    {
        let folder = tempfile::tempdir()?;

        assert_eq!(0, crate::drop::load_drop_counter());
        let db = Database::builder(&folder).open()?;
        assert_eq!(5, crate::drop::load_drop_counter());

        drop(db);
        assert_eq!(0, crate::drop::load_drop_counter());
    }

    {
        let folder = tempfile::tempdir()?;

        assert_eq!(0, crate::drop::load_drop_counter());
        let db = Database::builder(&folder).open()?;
        assert_eq!(5, crate::drop::load_drop_counter());

        let tree = db.keyspace("default", Default::default)?;
        assert_eq!(6, crate::drop::load_drop_counter());

        drop(tree);
        drop(db);
        assert_eq!(0, crate::drop::load_drop_counter());
    }

    {
        let folder = tempfile::tempdir()?;

        assert_eq!(0, crate::drop::load_drop_counter());
        let db = Database::builder(&folder).open()?;
        assert_eq!(5, crate::drop::load_drop_counter());

        let _tree = db.keyspace("default", Default::default)?;
        assert_eq!(6, crate::drop::load_drop_counter());

        let _tree2 = db.keyspace("different", Default::default)?;
        assert_eq!(7, crate::drop::load_drop_counter());
    }

    assert_eq!(0, crate::drop::load_drop_counter());

    Ok(())
}

#[cfg(feature = "__internal_whitebox")]
#[test]
#[ignore = "restore"]
fn whitebox_db_drop_2() -> crate::Result<()> {
    use crate::{Database, KeyspaceCreateOptions};

    let folder = tempfile::tempdir()?;

    {
        let db = Database::builder(&folder).open()?;

        let tree = db.keyspace("tree", KeyspaceCreateOptions::default)?;
        let tree2 = db.keyspace("tree1", KeyspaceCreateOptions::default)?;

        tree.insert("a", "a")?;
        tree2.insert("b", "b")?;

        tree.rotate_memtable_and_wait()?;
    }

    assert_eq!(0, crate::drop::load_drop_counter());

    Ok(())
}

#[test]
pub fn test_exotic_keyspace_names() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    let db = Database::builder(&folder).open()?;

    for name in ["hello$world", "hello#world", "hello.world", "hello_world"] {
        let tree = db.keyspace(name, KeyspaceCreateOptions::default)?;
        tree.insert("a", "a")?;
        assert_eq!(1, tree.len()?);
    }

    Ok(())
}

#[test]
#[expect(clippy::unwrap_used)]
fn recover_sealed_smoke_test() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;

    for i in 0_u128..3 {
        let db = Database::create_or_recover(Database::builder(folder.path()).into_config())?;

        let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;

        assert_eq!(i, tree.len()?.try_into().unwrap());

        tree.insert(i.to_be_bytes(), i.to_be_bytes())?;
        assert_eq!(i + 1, tree.len()?.try_into().unwrap());

        tree.rotate_memtable_and_wait()?;
    }

    Ok(())
}

#[test]
#[expect(clippy::unwrap_used)]
fn recover_sealed_order() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;

    {
        let db = Database::builder(folder.path())
            .worker_threads_unchecked(0)
            .open()?;

        let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;

        tree.insert("a", "a")?;
        tree.rotate_memtable()?;

        tree.insert("a", "b")?;
        tree.rotate_memtable()?;

        tree.insert("a", "c")?;
        tree.rotate_memtable()?;
    }

    {
        let db = Database::create_or_recover(Database::builder(folder.path()).into_config())?;

        let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;

        assert_eq!(b"c", &*tree.get("a")?.unwrap());
    }

    Ok(())
}

#[test]
#[expect(clippy::unwrap_used)]
fn recover_sealed_blob() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;

    for i in 0_u128..3 {
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
fn recover_sealed_pair_1() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;

    for i in 0_u128..3 {
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

// Regression test for https://github.com/fjall-rs/fjall/issues/328
#[test]
fn delete_keyspace_1() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;

    {
        let db = Database::builder(folder.path()).open()?;
        let _keep = db.keyspace("keep", KeyspaceCreateOptions::default)?;
        let a = db.keyspace("a", KeyspaceCreateOptions::default)?;
        a.insert("stale", "written to a")?;
        db.delete_keyspace(a)?;
    }

    {
        let db = Database::builder(folder.path()).open()?;
        let b = db.keyspace("b", KeyspaceCreateOptions::default)?;
        assert!(b.is_empty()?, "keyspace b should be empty");
    }

    {
        let db = Database::builder(folder.path()).open()?;
        let b = db.keyspace("b", KeyspaceCreateOptions::default)?;
        assert!(b.is_empty()?, "keyspace b should still be empty");
    }

    Ok(())
}
