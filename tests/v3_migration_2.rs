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

/// File names in a DB-level folder (`tables` or `blobs`): (legacy `{ks}_{id}` names, plain v4 names)
fn file_names(dir: &Path, folder: &str) -> std::io::Result<(Vec<String>, Vec<String>)> {
    let mut legacy = vec![];
    let mut v4 = vec![];

    for dirent in std::fs::read_dir(dir.join(folder))? {
        let name = dirent?.file_name().to_string_lossy().into_owned();

        if name.contains('_') {
            legacy.push(name);
        } else {
            v4.push(name);
        }
    }

    legacy.sort();
    v4.sort();

    Ok((legacy, v4))
}

/// Migrated v3 files are linked as `{keyspace ID}_{file ID}` (same content), and stay accessible;
/// files written by v4 afterwards get plain IDs
#[test]
fn v3_migration_file_names() -> fjall::Result<()> {
    let folder = "test_fixture/v3_keyspace_blobs";

    let dir = tempfile::tempdir()?;
    copy_dir_all(folder, &dir)?;

    let large_value = "v4".repeat(32_000);

    let keyspace_id = {
        let db = Database::builder(&dir).open()?;
        let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
        let keyspace_id = tree.id();

        assert_eq!(1, tree.table_count());
        assert_eq!(1, tree.blob_file_count());

        // NOTE: The v3 table and blob file of this keyspace (both with ID 0)
        let legacy_name = format!("{keyspace_id}_0");

        let (legacy_tables, v4_tables) = file_names(dir.path(), "tables")?;
        let (legacy_blob_files, v4_blob_files) = file_names(dir.path(), "blobs")?;

        assert!(legacy_tables.contains(&legacy_name), "{legacy_tables:?}");
        assert!(
            legacy_blob_files.contains(&legacy_name),
            "{legacy_blob_files:?}",
        );
        assert!(v4_tables.is_empty(), "{v4_tables:?}");
        assert!(v4_blob_files.is_empty(), "{v4_blob_files:?}");

        // NOTE: Linked, so the content is exactly the v3 file's
        let v3_folder = Path::new(folder)
            .join("keyspaces")
            .join(keyspace_id.to_string());

        assert_eq!(
            std::fs::read(v3_folder.join("tables").join("0"))?,
            std::fs::read(dir.path().join("tables").join(&legacy_name))?,
        );
        assert_eq!(
            std::fs::read(v3_folder.join("blobs").join("0"))?,
            std::fs::read(dir.path().join("blobs").join(&legacy_name))?,
        );

        // NOTE: Accessible through the legacy names
        assert_eq!(Some(b"avalue".as_slice()), tree.get("a")?.as_deref());

        // NOTE: A v4 table + blob file
        tree.insert("v4", &large_value, fjall::DurabilityLevel::SyncAll)?;
        tree.flush(true)?;

        assert_eq!(2, tree.table_count());
        assert_eq!(2, tree.blob_file_count());

        keyspace_id
    };

    let legacy_name = format!("{keyspace_id}_0");

    let (legacy_tables, v4_tables) = file_names(dir.path(), "tables")?;
    let (legacy_blob_files, v4_blob_files) = file_names(dir.path(), "blobs")?;

    // NOTE: The v4 files have plain IDs, the legacy files are untouched
    assert_eq!(1, v4_tables.len());
    assert_eq!(1, v4_blob_files.len());
    assert!(v4_tables[0].parse::<u64>().is_ok());
    assert!(v4_blob_files[0].parse::<u64>().is_ok());
    assert!(legacy_tables.contains(&legacy_name));
    assert!(
        legacy_blob_files.contains(&legacy_name),
        "{legacy_blob_files:?}",
    );

    {
        let db = Database::builder(&dir).open()?;
        let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;

        assert_eq!(2, tree.table_count());
        assert_eq!(2, tree.blob_file_count());

        // NOTE: Both the migrated and the new item are readable after recovery
        assert_eq!(Some(b"avalue".as_slice()), tree.get("a")?.as_deref());
        assert_eq!(Some(large_value.as_bytes()), tree.get("v4")?.as_deref());
    }

    Ok(())
}

/// v3 fixture `v3_journal_compression`, written by fjall v3 (main) with
/// `journal_compression(CompressionType::Lz4)` (values >= 4 KiB are compressed in the journal),
/// nothing flushed, so the migration has to decompress the journal items:
///
/// ```ignore
/// // NOTE: Compressible value
/// fn big(byte: u8, len: usize) -> Vec<u8> {
///     (0..len).map(|i| byte.wrapping_add((i % 16) as u8)).collect()
/// }
///
/// let ks = db.keyspace("default", KeyspaceCreateOptions::default)?;
/// ks.insert("small", "smallvalue")?;
/// ks.insert("big", big(b'a', 8_192))?;
/// ks.insert("removed", big(b'r', 8_192))?;
/// ks.remove("removed")?;
///
/// let mut batch = db.batch();
/// batch.insert(&ks, "batch_big", big(b'b', 16_384));
/// batch.insert(&ks, "batch_small", "batchsmall");
/// batch.commit()?;
///
/// db.persist(PersistMode::SyncAll)?;
/// ```
///
/// NOTE: The journal is 528 bytes, so the big values really are compressed
#[test]
fn v3_migration_journal_compression() -> fjall::Result<()> {
    fn big(byte: u8, len: usize) -> Vec<u8> {
        (0..len).map(|i| byte.wrapping_add((i % 16) as u8)).collect()
    }

    let dir = tempfile::tempdir()?;
    copy_dir_all("test_fixture/v3_journal_compression", &dir)?;

    for i in 0..3 {
        log::info!("[{i}] recovering");

        let db = Database::builder(&dir).open()?;
        let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;

        assert_eq!(4, tree.len()?);

        assert_eq!(Some(b"smallvalue".as_slice()), tree.get("small")?.as_deref());
        assert_eq!(Some(big(b'a', 8_192).as_slice()), tree.get("big")?.as_deref());
        assert!(tree.get("removed")?.is_none());
        assert_eq!(
            Some(big(b'b', 16_384).as_slice()),
            tree.get("batch_big")?.as_deref(),
        );
        assert_eq!(
            Some(b"batchsmall".as_slice()),
            tree.get("batch_small")?.as_deref(),
        );
    }

    Ok(())
}
