use super::*;
use crate::{DurabilityLevel::Buffer, KeyspaceCreateOptions, KvSeparationOptions};
use test_log::test;

#[test]
fn create_backup_copies_wal_and_files() -> crate::Result<()> {
    let db_folder = tempfile::tempdir()?;
    let backup_folder = tempfile::tempdir()?;
    let backups = backup_folder.path();

    let db = Database::builder(db_folder.path())
        .worker_threads_unchecked(0)
        .open()?;

    let standard = db.keyspace("standard", KeyspaceCreateOptions::default)?;

    let separated = db.keyspace("separated", || {
        KeyspaceCreateOptions::default()
            .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
    })?;

    // NOTE: Flushed, so in tables (and blob files)
    standard.insert("a", "1", Buffer)?;
    separated.insert("a", "1", Buffer)?;
    standard.flush(true)?;
    separated.flush(true)?;

    // NOTE: Not flushed, only in the WAL
    standard.insert("b", "2", Buffer)?;
    separated.insert("b", "2", Buffer)?;

    let mut engine = Backup::open(backups)?;
    assert_eq!(1, engine.create_backup(&db, BackupMode::KeepLogs)?);

    let private = backups.join(PRIVATE_FOLDER).join("1");
    assert!(private.join("current").exists(), "should be committed");
    assert!(private.join("v0").exists(), "should have a manifest");
    assert!(
        std::fs::read_dir(&private)?.any(|entry| {
            entry.is_ok_and(|entry| entry.path().extension().is_some_and(|ext| ext == "hwal"))
        }),
        "should have WAL fragments",
    );

    // NOTE: Every table and blob file of the database is in the backup
    assert_eq!(1, standard.table_count());
    assert_eq!(1, separated.table_count());
    assert_eq!(1, separated.blob_file_count());

    for folder in [TABLES_FOLDER, BLOBS_FOLDER] {
        for entry in std::fs::read_dir(db_folder.path().join(folder))? {
            let name = entry?.file_name();

            let path = backups
                .join(SHARED_FOLDER)
                .join(folder)
                .join(&name)
                .exists();

            assert!(path, "{name:?} should be in the backup");
        }
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;

        let table = std::fs::read_dir(db_folder.path().join(TABLES_FOLDER))?
            .next()
            .expect("should have a table")?;

        let shared_table = std::fs::metadata(
            backups
                .join(SHARED_FOLDER)
                .join(TABLES_FOLDER)
                .join(table.file_name()),
        )?;

        assert_eq!(
            table.metadata()?.ino(),
            shared_table.ino(),
            "should be hard-linked"
        );
    }

    // NOTE: The WAL pin and the snapshot are released again
    assert!(!db.supervisor.ledger()?.wal.is_pinned());
    assert_eq!(0, db.supervisor.snapshot_tracker.open_snapshots());

    let [backup] = engine.backups() else {
        panic!("should have exactly one backup");
    };
    assert_eq!(1, backup.id);
    assert_eq!(2, backup.table_count);
    assert_eq!(1, backup.blob_file_count);

    let listed = engine.backups().to_vec();
    drop(engine);
    assert_eq!(listed, Backup::open(backups)?.backups());

    Ok(())
}

/// Files the folder already has (from the previous backup) are not copied again
#[test]
fn create_backup_is_incremental() -> crate::Result<()> {
    let db_folder = tempfile::tempdir()?;
    let backup_folder = tempfile::tempdir()?;
    let shared_tables = backup_folder.path().join(SHARED_FOLDER).join(TABLES_FOLDER);

    let db = Database::builder(db_folder.path())
        .worker_threads_unchecked(0)
        .open()?;

    let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;

    tree.insert("a", "1", Buffer)?;
    tree.flush(true)?;
    tree.insert("b", "2", Buffer)?;
    tree.flush(true)?;

    let mut engine = Backup::open(backup_folder.path())?;
    engine.create_backup(&db, BackupMode::KeepLogs)?;
    assert_eq!(2, std::fs::read_dir(&shared_tables)?.count());

    tree.insert("c", "3", Buffer)?;
    tree.flush(true)?;
    tree.major_compact()?;
    assert_eq!(1, tree.table_count());

    assert_eq!(2, engine.create_backup(&db, BackupMode::KeepLogs)?);

    // NOTE: The first backup's two tables stay, only the compacted table is added
    // (the table of "c" was compacted away before it was backed up)
    assert_eq!(3, std::fs::read_dir(&shared_tables)?.count());

    let [first, second] = engine.backups() else {
        panic!("should have two backups");
    };
    assert_eq!(2, first.table_count);
    assert_eq!(1, second.table_count);

    Ok(())
}

/// With `BackupMode::Flush`, everything is in tables, so the backup has no WAL
#[test]
fn create_backup_flush_mode_has_no_wal() -> crate::Result<()> {
    let db_folder = tempfile::tempdir()?;
    let backup_folder = tempfile::tempdir()?;

    let db = Database::builder(db_folder.path())
        .worker_threads_unchecked(0)
        .open()?;

    let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
    tree.insert("a", "1", Buffer)?;

    let mut backup = Backup::open(backup_folder.path())?;
    backup.create_backup(&db, BackupMode::Flush)?;

    assert!(tree.tree.active_memtable().is_empty());
    assert_eq!(0, tree.sealed_memtable_count());

    let private = backup_folder.path().join(PRIVATE_FOLDER).join("1");
    assert!(private.join("current").exists());
    assert!(
        !std::fs::read_dir(&private)?.any(|entry| {
            entry.is_ok_and(|entry| entry.path().extension().is_some_and(|ext| ext == "hwal"))
        }),
        "should have no WAL fragments",
    );
    assert_eq!(1, backup.backups().first().map_or(0, |b| b.table_count));

    Ok(())
}

/// Like `Database::flush_all`, a flushing backup leaves no sealed WAL fragments behind
#[test]
fn create_backup_flush_mode_prunes_wal() -> crate::Result<()> {
    let db_folder = tempfile::tempdir()?;
    let backup_folder = tempfile::tempdir()?;

    let db = Database::builder(db_folder.path())
        .worker_threads_unchecked(0)
        .open()?;

    let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;

    let mut backup = Backup::open(backup_folder.path())?;

    for i in 0..3_u32 {
        tree.insert(i.to_be_bytes(), "1", Buffer)?;

        backup.create_backup(&db, BackupMode::Flush)?;

        assert_eq!(
            0,
            db.supervisor.ledger()?.wal.sealed_fragment_paths().count(),
            "backup #{}",
            i + 1,
        );
    }

    Ok(())
}

/// Pruning is skipped while the WAL is pinned, the backup catches up after unpinning
#[test]
fn create_backup_prunes_wal_after_unpinning() -> crate::Result<()> {
    let db_folder = tempfile::tempdir()?;
    let backup_folder = tempfile::tempdir()?;

    let db = Database::builder(db_folder.path())
        .worker_threads_unchecked(0)
        .open()?;

    let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;

    let mut backup = Backup::open(backup_folder.path())?;

    tree.insert("a", "1", Buffer)?;
    tree.flush(true)?;

    // NOTE: The fragment is flushed, but only sealed by the backup's rotation,
    // so nothing else prunes it
    backup.create_backup(&db, BackupMode::KeepLogs)?;

    assert_eq!(
        0,
        db.supervisor.ledger()?.wal.sealed_fragment_paths().count(),
    );

    Ok(())
}

/// A failed backup is not committed, and releases everything it held
#[test]
fn create_backup_of_poisoned_db_fails() -> crate::Result<()> {
    let db_folder = tempfile::tempdir()?;
    let backup_folder = tempfile::tempdir()?;

    let db = Database::builder(db_folder.path())
        .worker_threads_unchecked(0)
        .open()?;
    db.keyspace("default", KeyspaceCreateOptions::default)?
        .insert("a", "1", Buffer)?;

    db.is_poisoned.poison();

    {
        let mut backup = Backup::open(backup_folder.path())?;

        assert!(matches!(
            backup.create_backup(&db, BackupMode::KeepLogs),
            Err(crate::Error::Poisoned),
        ));
        assert_eq!(0, backup.backups().len());
        assert!(!db.supervisor.ledger()?.wal.is_pinned());
    }
    assert_eq!(0, Backup::open(backup_folder.path())?.backups().len());

    Ok(())
}
