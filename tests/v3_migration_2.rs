use fjall::{Database, KeyspaceCreateOptions, KvSeparationOptions};
use std::path::Path;
use test_log::test;

fn copy_dir_all(src: impl AsRef<Path>, dst: impl AsRef<Path>) -> std::io::Result<()> {
    std::fs::create_dir_all(&dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let ty = entry.file_type()?;
        if ty.is_dir() {
            copy_dir_all(entry.path(), dst.as_ref().join(entry.file_name()))?;
        } else {
            std::fs::copy(entry.path(), dst.as_ref().join(entry.file_name()))?;
        }
    }
    Ok(())
}

// V3 -> V4 migration with flushed tables and blob files
#[test]
fn v3_migration_tables() -> fjall::Result<()> {
    let folder = "test_fixture/v3_keyspace_tables";

    let dir = tempfile::tempdir()?;
    copy_dir_all(folder, &dir)?;

    let db = Database::builder(&dir).open()?;
    let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
    let tree2 = db.keyspace("default2", KeyspaceCreateOptions::default)?;

    /*
        tree.insert("hello", "world")?;
        tree2.insert("hello", "world")?;

        tree.clear()?;
        tree2.clear()?;

        tree.insert("a", "avalue")?;
        tree.insert("b", "bvalue")?;
        tree.insert("c", "cvalue")?;

        tree2.insert("d", "dvalue")?;

        tree.rotate_memtable_and_wait()?;
        tree2.rotate_memtable_and_wait()?;
    */

    assert!(tree.table_count() == 1);
    assert!(tree2.table_count() == 1);

    assert!(!tree.contains_key("hello")?);
    assert!(!tree2.contains_key("hello")?);

    assert_eq!(Some(b"avalue".as_slice()), tree.get("a")?.as_deref());
    assert_eq!(Some(b"bvalue".as_slice()), tree.get("b")?.as_deref());
    assert_eq!(Some(b"cvalue".as_slice()), tree.get("c")?.as_deref());

    assert_eq!(Some(b"dvalue".as_slice()), tree2.get("d")?.as_deref());

    Ok(())
}

// V3 -> V4 migration with flushed tables and blob files
#[test]
fn v3_migration_blobs() -> fjall::Result<()> {
    let folder = "test_fixture/v3_keyspace_blobs";

    let dir = tempfile::tempdir()?;
    copy_dir_all(folder, &dir)?;

    let db = Database::builder(&dir).open()?;
    let tree = db.keyspace("default", || {
        KeyspaceCreateOptions::default()
            .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
    })?;
    let tree2 = db.keyspace("default2", || {
        KeyspaceCreateOptions::default()
            .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
    })?;

    /*
        tree.insert("hello", "world")?;
        tree2.insert("hello", "world")?;

        tree.clear()?;
        tree2.clear()?;

        tree.insert("a", "avalue")?;
        tree.insert("b", "bvalue")?;
        tree.insert("c", "cvalue")?;

        tree2.insert("d", "dvalue")?;

        tree.rotate_memtable_and_wait()?;
        tree2.rotate_memtable_and_wait()?;
    */

    assert!(tree.table_count() == 1);
    assert!(tree.blob_file_count() == 1);
    assert!(tree2.table_count() == 1);
    assert!(tree2.blob_file_count() == 1);

    assert!(!tree.contains_key("hello")?);
    assert!(!tree2.contains_key("hello")?);

    assert_eq!(Some(b"avalue".as_slice()), tree.get("a")?.as_deref());
    assert_eq!(Some(b"bvalue".as_slice()), tree.get("b")?.as_deref());
    assert_eq!(Some(b"cvalue".as_slice()), tree.get("c")?.as_deref());

    assert_eq!(Some(b"dvalue".as_slice()), tree2.get("d")?.as_deref());

    Ok(())
}
