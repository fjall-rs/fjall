use crate::{
    AbstractTree, Database, DurabilityLevel::Buffer, KeyspaceCreateOptions, KvSeparationOptions,
};
use test_log::test;

#[test_log::test]
fn clear_recover_sealed() -> crate::Result<()> {
    use crate::{Database, KeyspaceCreateOptions};

    let folder = tempfile::tempdir()?;

    {
        let db = Database::builder(&folder).open()?;
        assert_eq!(1, db.wal_count());

        let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
        assert!(tree.is_empty()?);

        tree.insert("a", "a", crate::DurabilityLevel::Buffer)?;
        assert!(tree.contains_key("a")?);

        tree.clear()?;
        assert!(tree.is_empty()?);

        tree.flush(true)?;
        assert!(tree.is_empty()?);

        db.rotate_wal()?;

        // NOTE: The recovery below replays items from two WAL fragments
        assert_eq!(2, db.wal_count());

        tree.insert("b", "a", crate::DurabilityLevel::Buffer)?;
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

        tree.flush(true)?;
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
        tree.insert("a", "a", crate::DurabilityLevel::Buffer)?;
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

        tree.insert(
            i.to_be_bytes(),
            i.to_be_bytes(),
            crate::DurabilityLevel::Buffer,
        )?;
        assert_eq!(i + 1, tree.len()?.try_into().unwrap());

        tree.flush(true)?;
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

        tree.insert("a", "a", crate::DurabilityLevel::Buffer)?;
        tree.rotate_memtable()?;

        tree.insert("a", "b", crate::DurabilityLevel::Buffer)?;
        tree.rotate_memtable()?;

        tree.insert("a", "c", crate::DurabilityLevel::Buffer)?;
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
        let db = Database::builder(folder.path())
            .worker_threads_unchecked(0)
            .open()?;

        let tree = db.keyspace("default", || {
            KeyspaceCreateOptions::default()
                .max_memtable_size(1_000)
                .with_kv_separation(Some(KvSeparationOptions::default()))
        })?;

        assert_eq!(i, tree.len()?.try_into().unwrap());

        tree.insert(
            i.to_be_bytes(),
            i.to_be_bytes().repeat(1_024),
            crate::DurabilityLevel::Buffer,
        )?;
        assert_eq!(i + 1, tree.len()?.try_into().unwrap());

        tree.flush(true)?;
    }

    Ok(())
}

#[test]
#[expect(clippy::unwrap_used)]
fn recover_sealed_pair_1() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;

    for i in 0_u128..3 {
        let db = Database::builder(folder.path())
            .worker_threads_unchecked(0)
            .disable_write_stalls(true)
            .open()?;

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

        tree.flush(true)?;
    }

    Ok(())
}

#[test]
fn recover_sealed_partially_flushed_batch() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;

    let unflushed_size = {
        let db = Database::builder(folder.path()).open()?;
        assert_eq!(1, db.wal_count());

        let a = db.keyspace("a", KeyspaceCreateOptions::default)?;
        let b = db.keyspace("b", KeyspaceCreateOptions::default)?;

        let mut batch = db.batch();
        for x in 0_u64..100 {
            batch.insert(&a, x.to_be_bytes(), "a");
            batch.insert(&b, x.to_be_bytes(), "b");
        }
        batch.commit()?;

        a.flush(true)?;

        let mut batch = db.batch();
        for x in 100_u64..200 {
            batch.insert(&a, x.to_be_bytes(), "a");
        }
        batch.commit()?;

        db.rotate_wal()?;

        // NOTE: The recovery below replays items from two WAL fragments
        assert_eq!(2, db.wal_count());

        a.tree.active_memtable().size() + b.tree.active_memtable().size()
    };

    {
        let db = Database::builder(folder.path())
            .worker_threads_unchecked(0)
            .open()?;
        let a = db.keyspace("a", KeyspaceCreateOptions::default)?;
        let b = db.keyspace("b", KeyspaceCreateOptions::default)?;

        assert_eq!(unflushed_size, db.write_buffer_size());
        assert_eq!(200, a.len()?);
        assert_eq!(100, b.len()?);
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
        a.insert("stale", "written to a", crate::DurabilityLevel::Buffer)?;
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

/// Flushing all keyspaces removes the covered WAL and allows reopening without replay.
#[test]
fn flush_all_materializes_wal() -> crate::Result<()> {
    use crate::AbstractTree;

    let folder = tempfile::tempdir()?;

    {
        let db = Database::builder(&folder)
            .worker_threads_unchecked(0)
            .open()?;

        let first = db.keyspace("first", KeyspaceCreateOptions::default)?;
        let second = db.keyspace("second", KeyspaceCreateOptions::default)?;

        {
            let mut batch = db.batch();
            batch.insert(&first, "a", "1");
            batch.insert(&second, "b", "2");
            batch.commit()?;
        }
        first.rotate_memtable()?;
        first.insert("c", "3", Buffer)?;

        db.rotate_wal()?;
        assert_eq!(2, db.wal_count());

        db.flush_all()?;
        assert_eq!(1, db.wal_count());

        assert!(first.tree.active_memtable().is_empty());
        assert!(second.tree.active_memtable().is_empty());
        assert_eq!(0, first.tree.sealed_memtable_count());
        assert_eq!(0, second.tree.sealed_memtable_count());

        first.insert("d", "4", Buffer)?;

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

/// A clear drops the active and the sealed memtables (which are then never flushed),
/// so it frees their write buffer bytes, but not those of other keyspaces.
#[test]
fn clear_frees_write_buffer() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;

    let db = Database::builder(&folder)
        .worker_threads_unchecked(0)
        .open()?;

    let a = db.keyspace("a", KeyspaceCreateOptions::default)?;
    let b = db.keyspace("b", KeyspaceCreateOptions::default)?;
    assert_eq!(0, db.write_buffer_size());

    b.insert("b", "b", Buffer)?;
    let b_size = db.write_buffer_size();
    assert!(b_size > 0);

    // NOTE: One sealed memtable, and an active one
    a.insert("a", "a", Buffer)?;
    assert!(a.rotate_memtable()?);
    a.insert("a2", "a", Buffer)?;
    assert_eq!(1, a.sealed_memtable_count());
    assert!(db.write_buffer_size() > b_size);

    a.clear()?;
    assert_eq!(0, a.sealed_memtable_count());
    assert_eq!(
        b_size,
        db.write_buffer_size(),
        "only b's bytes should be left"
    );

    b.clear()?;
    assert_eq!(0, db.write_buffer_size());

    Ok(())
}

/// Deleting a keyspace frees the write buffer bytes of its (active and sealed) memtables.
#[test]
fn delete_keyspace_frees_write_buffer() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;

    let db = Database::builder(&folder)
        .worker_threads_unchecked(0)
        .open()?;

    let a = db.keyspace("a", KeyspaceCreateOptions::default)?;

    a.insert("a", "a", Buffer)?;
    assert!(a.rotate_memtable()?);
    a.insert("a2", "a", Buffer)?;
    assert!(db.write_buffer_size() > 0);

    db.delete_keyspace(a)?;
    assert_eq!(0, db.write_buffer_size());

    Ok(())
}

/// Regression test for <https://github.com/fjall-rs/fjall/issues/288>
#[test]
fn clear_does_not_block_wal_pruning() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;

    let db = Database::builder(&folder)
        .worker_threads_unchecked(0)
        .open()?;

    let cold = db.keyspace("cold", KeyspaceCreateOptions::default)?;
    let hot = db.keyspace("hot", KeyspaceCreateOptions::default)?;

    // NOTE: WAL fragment #0 needs cold to be flushed (or cleared)
    cold.insert("a", "a", Buffer)?;
    db.rotate_wal()?;
    cold.clear()?;

    for _ in 0..5 {
        hot.insert("a", "abcdef", Buffer)?;
        db.rotate_wal()?;

        // NOTE: Prunes the WAL afterwards
        hot.flush(true)?;
    }

    // NOTE: Everything is flushed or cleared, so only the active WAL fragment is needed
    assert_eq!(1, db.wal_count());

    Ok(())
}
