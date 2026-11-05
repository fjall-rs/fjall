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

    for i in 0..5 {
        log::info!("[{i}] recovering");

        let db = Database::builder(&dir).open()?;

        // NOTE: The highest seqno in the v3 fixture is 12,
        // so migrated data must be visible, and new writes must get higher seqnos
        assert!(
            db.visible_seqno() > 12,
            "visible seqno: {}",
            db.visible_seqno(),
        );
        assert!(db.next_seqno() > 12, "next seqno: {}", db.next_seqno());

        let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
        let tree2 = db.keyspace("default2", KeyspaceCreateOptions::default)?;

        assert_eq!(1, tree.table_count());
        assert_eq!(1, tree2.table_count());

        assert!(!tree.contains_key("hello")?);
        assert!(!tree2.contains_key("hello")?);

        assert_eq!(Some(b"avalue".as_slice()), tree.get("a")?.as_deref());
        assert_eq!(Some(b"bvalue".as_slice()), tree.get("b")?.as_deref());
        assert_eq!(Some(b"cvalue".as_slice()), tree.get("c")?.as_deref());

        assert_eq!(Some(b"dvalue".as_slice()), tree2.get("d")?.as_deref());
    }

    Ok(())
}

// V3 -> V4 migration with flushed tables and blob files
#[test]
fn v3_migration_blobs() -> fjall::Result<()> {
    let folder = "test_fixture/v3_keyspace_blobs";

    let dir = tempfile::tempdir()?;
    copy_dir_all(folder, &dir)?;

    for i in 0..5 {
        log::info!("[{i}] recovering");

        let db = Database::builder(&dir).open()?;

        // NOTE: The highest seqno in the v3 fixture is 12,
        // so migrated data must be visible, and new writes must get higher seqnos
        assert!(
            db.visible_seqno() > 12,
            "visible seqno: {}",
            db.visible_seqno(),
        );
        assert!(db.next_seqno() > 12, "next seqno: {}", db.next_seqno());

        let tree = db.keyspace("default", || {
            KeyspaceCreateOptions::default()
                .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
        })?;
        let tree2 = db.keyspace("default2", || {
            KeyspaceCreateOptions::default()
                .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
        })?;

        assert!(tree.is_kv_separated());
        assert!(tree2.is_kv_separated());

        assert_eq!(1, tree.table_count());
        assert_eq!(1, tree.blob_file_count());
        assert_eq!(1, tree2.table_count());
        assert_eq!(1, tree2.blob_file_count());

        assert!(!tree.contains_key("hello")?);
        assert!(!tree2.contains_key("hello")?);

        assert_eq!(Some(b"avalue".as_slice()), tree.get("a")?.as_deref());
        assert_eq!(Some(b"bvalue".as_slice()), tree.get("b")?.as_deref());
        assert_eq!(Some(b"cvalue".as_slice()), tree.get("c")?.as_deref());

        assert_eq!(Some(b"dvalue".as_slice()), tree2.get("d")?.as_deref());
    }

    Ok(())
}

/// Migrated v3 files (named `<keyspace ID>_<ID>`) in a DB-level folder
fn legacy_files(dir: &Path, folder: &str) -> std::io::Result<Vec<String>> {
    let mut names = std::fs::read_dir(dir.join(folder))?
        .map(|e| e.map(|e| e.file_name().to_string_lossy().into_owned()))
        .filter(|name| name.as_ref().map_or(true, |name| name.contains('_')))
        .collect::<Result<Vec<_>, _>>()?;
    names.sort();
    Ok(names)
}

// After a V3 -> V4 migration, the v3 tree folders are gone, and dropping migrated tables
// (here: clearing a keyspace after a v4 reopen) deletes their files in the DB-level folders
#[test]
fn v3_migration_drops_legacy_files() -> fjall::Result<()> {
    let folder = "test_fixture/v3_keyspace_tables";

    let dir = tempfile::tempdir()?;
    copy_dir_all(folder, &dir)?;

    {
        let _db = Database::builder(&dir).open()?;
    }

    assert!(
        !dir.path().join("keyspaces").try_exists()?,
        "v3 keyspace folders should be removed after the migration",
    );
    assert_eq!(2, legacy_files(dir.path(), "tables")?.len());

    {
        let db = Database::builder(&dir).open()?;
        let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
        let tree2 = db.keyspace("default2", KeyspaceCreateOptions::default)?;

        assert_eq!(Some(b"avalue".as_slice()), tree.get("a")?.as_deref());

        tree.clear()?;
        assert_eq!(0, tree.table_count());

        // NOTE: The other keyspace still reads its migrated table
        assert_eq!(Some(b"dvalue".as_slice()), tree2.get("d")?.as_deref());
    }

    assert_eq!(
        1,
        legacy_files(dir.path(), "tables")?.len(),
        "the cleared keyspace's migrated table should be deleted",
    );

    {
        let db = Database::builder(&dir).open()?;
        let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
        let tree2 = db.keyspace("default2", KeyspaceCreateOptions::default)?;

        assert!(tree.get("a")?.is_none());
        assert_eq!(Some(b"dvalue".as_slice()), tree2.get("d")?.as_deref());
    }

    Ok(())
}
