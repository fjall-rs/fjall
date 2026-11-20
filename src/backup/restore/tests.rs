use super::*;
use crate::{
    BackupMode, Database, DurabilityLevel::Buffer, KeyspaceCreateOptions, KvSeparationOptions,
};
use test_log::test;

#[test]
fn restore_backup_restores_state_at_backup() -> crate::Result<()> {
    for mode in [BackupMode::KeepLogs, BackupMode::Flush] {
        let db_folder = tempfile::tempdir()?;
        let backup_folder = tempfile::tempdir()?;
        let restore_folder = tempfile::tempdir()?;

        let db = Database::builder(db_folder.path())
            .worker_threads_unchecked(0)
            .open()?;

        let standard = db.keyspace("standard", KeyspaceCreateOptions::default)?;

        let separated = db.keyspace("separated", || {
            KeyspaceCreateOptions::default()
                .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
        })?;

        for keyspace in [&standard, &separated] {
            keyspace.insert("a", "flushed", Buffer)?;
            keyspace.insert("b", "removed", Buffer)?;
            keyspace.flush(true)?;

            keyspace.insert("c", "only in the WAL", Buffer)?;
            keyspace.remove("b", Buffer)?;
        }

        let mut backup = Backup::open(backup_folder.path())?;
        let id = backup.create_backup(&db, mode)?;

        // NOTE: After the backup, so not part of it
        standard.insert("d", "after the backup", Buffer)?;

        backup.restore_backup(id, restore_folder.path())?;

        let restored = Database::builder(restore_folder.path())
            .worker_threads_unchecked(0)
            .open()?;

        for name in ["standard", "separated"] {
            let keyspace = restored.keyspace(name, KeyspaceCreateOptions::default)?;

            assert_eq!(
                Some(b"flushed".as_slice()),
                keyspace.get("a")?.as_deref(),
                "{mode:?}"
            );
            assert_eq!(None, keyspace.get("b")?, "{mode:?}");
            assert_eq!(
                Some(b"only in the WAL".as_slice()),
                keyspace.get("c")?.as_deref(),
                "{mode:?}"
            );
            assert_eq!(None, keyspace.get("d")?, "{mode:?}");
        }

        // NOTE: The restored database is usable
        let keyspace = restored.keyspace("standard", KeyspaceCreateOptions::default)?;
        keyspace.insert("new", "value", Buffer)?;
        keyspace.flush(true)?;

        assert_eq!(Some(b"value".as_slice()), keyspace.get("new")?.as_deref());
    }

    Ok(())
}

/// Restoring an older backup (or one with few files) still continues with IDs
/// above every file in the backup folder
#[test]
fn restore_backup_continues_above_all_file_ids() -> crate::Result<()> {
    let db_folder = tempfile::tempdir()?;
    let backup_folder = tempfile::tempdir()?;
    let restore_folder = tempfile::tempdir()?;
    let shared = backup_folder.path().join(super::super::SHARED_FOLDER);

    let db = Database::builder(db_folder.path())
        .worker_threads_unchecked(0)
        .open()?;

    let standard = db.keyspace("standard", KeyspaceCreateOptions::default)?;

    let separated = db.keyspace("separated", || {
        KeyspaceCreateOptions::default()
            .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
    })?;

    let mut backup = Backup::open(backup_folder.path())?;

    for keyspace in [&standard, &separated] {
        keyspace.insert("a", "1", Buffer)?;
        keyspace.flush(true)?;
    }
    let first = backup.create_backup(&db, BackupMode::KeepLogs)?;

    // NOTE: More tables and blob files, which only the second backup has
    for keyspace in [&standard, &separated] {
        keyspace.insert("b", "2", Buffer)?;
        keyspace.flush(true)?;
        keyspace.insert("c", "3", Buffer)?;
        keyspace.flush(true)?;
    }
    backup.create_backup(&db, BackupMode::KeepLogs)?;

    // NOTE: Roll back to the first backup
    backup.restore_backup(first, restore_folder.path())?;

    let restored = Database::builder(restore_folder.path())
        .worker_threads_unchecked(0)
        .open()?;
    for name in ["standard", "separated"] {
        let keyspace = restored.keyspace(name, KeyspaceCreateOptions::default)?;
        keyspace.insert("new", "value", Buffer)?;
        keyspace.flush(true)?;
    }

    let highest_id = |folder: &Path| -> crate::Result<Option<u64>> {
        let mut highest = None;

        for entry in std::fs::read_dir(folder)? {
            let id = entry?
                .file_name()
                .to_str()
                .and_then(|name| name.parse().ok());
            highest = highest.max(id);
        }

        Ok(highest)
    };

    assert!(
        highest_id(&restore_folder.path().join(TABLES_FOLDER))?
            > highest_id(&shared.join(TABLES_FOLDER))?,
        "new tables should get IDs above every table in the backup folder",
    );
    assert!(
        highest_id(&restore_folder.path().join(BLOBS_FOLDER))?
            > highest_id(&shared.join(BLOBS_FOLDER))?,
        "new blob files should get IDs above every blob file in the backup folder",
    );

    Ok(())
}

/// The restored database takes over the backup folder
#[test]
fn restore_backup_restored_db_can_back_up_into_same_folder() -> crate::Result<()> {
    let db_folder = tempfile::tempdir()?;
    let backup_folder = tempfile::tempdir()?;
    let first_folder = tempfile::tempdir()?;
    let second_folder = tempfile::tempdir()?;

    let db = Database::builder(db_folder.path())
        .worker_threads_unchecked(0)
        .open()?;

    db.keyspace("default", KeyspaceCreateOptions::default)?
        .insert("a", "1", Buffer)?;

    let mut backup = Backup::open(backup_folder.path())?;
    let first = backup.create_backup(&db, BackupMode::KeepLogs)?;
    drop(db);

    backup.restore_backup(first, first_folder.path())?;

    let restored = Database::builder(first_folder.path())
        .worker_threads_unchecked(0)
        .open()?;

    restored
        .keyspace("default", KeyspaceCreateOptions::default)?
        .insert("b", "2", Buffer)?;

    let second = backup.create_backup(&restored, BackupMode::KeepLogs)?;
    backup.restore_backup(second, second_folder.path())?;

    let tree = Database::builder(second_folder.path())
        .worker_threads_unchecked(0)
        .open()?
        .keyspace("default", KeyspaceCreateOptions::default)?;

    assert_eq!(Some(b"1".as_slice()), tree.get("a")?.as_deref());
    assert_eq!(Some(b"2".as_slice()), tree.get("b")?.as_deref());

    Ok(())
}

#[test]
fn restore_backup_errors() -> crate::Result<()> {
    let db_folder = tempfile::tempdir()?;
    let backup_folder = tempfile::tempdir()?;
    let restore_folder = tempfile::tempdir()?;

    let db = Database::builder(db_folder.path())
        .worker_threads_unchecked(0)
        .open()?;

    db.keyspace("default", KeyspaceCreateOptions::default)?
        .insert("a", "1", Buffer)?;

    let mut backup = Backup::open(backup_folder.path())?;
    let id = backup.create_backup(&db, BackupMode::KeepLogs)?;

    assert!(
        backup
            .restore_backup(id + 1, restore_folder.path().join("unknown"))
            .is_err(),
        "unknown backup",
    );

    std::fs::write(restore_folder.path().join("existing"), b"")?;

    assert!(
        backup.restore_backup(id, restore_folder.path()).is_err(),
        "folder is not empty",
    );
    assert!(
        !restore_folder.path().join(LOCK_FILE).exists(),
        "should not leave a lock file in a non-empty folder",
    );

    Ok(())
}

/// The restore folder is locked while files are copied
#[test]
fn restore_backup_locks_restore_folder() -> crate::Result<()> {
    let db_folder = tempfile::tempdir()?;
    let backup_folder = tempfile::tempdir()?;
    let restore_folder = tempfile::tempdir()?;

    let db = Database::builder(db_folder.path())
        .worker_threads_unchecked(0)
        .open()?;

    db.keyspace("default", KeyspaceCreateOptions::default)?
        .insert("a", "1", Buffer)?;

    let mut backup = Backup::open(backup_folder.path())?;
    let id = backup.create_backup(&db, BackupMode::KeepLogs)?;

    {
        // NOTE: Like a database (or another restore) holding the folder
        let _lock = LockedFileGuard::create_new(&restore_folder.path().join(LOCK_FILE))?;

        assert!(matches!(
            backup.restore_backup(id, restore_folder.path()),
            Err(crate::Error::Locked),
        ));
        assert!(
            !restore_folder.path().join(TABLES_FOLDER).exists(),
            "should not write anything into a locked folder",
        );
    }

    backup.restore_backup(id, restore_folder.path())?;

    let tree = Database::builder(restore_folder.path())
        .worker_threads_unchecked(0)
        .open()?
        .keyspace("default", KeyspaceCreateOptions::default)?;

    assert_eq!(Some(b"1".as_slice()), tree.get("a")?.as_deref());

    Ok(())
}

/// A seqno handed out without being written anywhere (here: a compaction with nothing to do)
/// right before the backup: the restored database still continues with the source's seqnos
#[test]
fn restore_backup_keeps_seqno_without_write() -> crate::Result<()> {
    let db_folder = tempfile::tempdir()?;
    let backup_folder = tempfile::tempdir()?;
    let restore_folder = tempfile::tempdir()?;

    let db = Database::builder(db_folder.path())
        .worker_threads_unchecked(0)
        .open()?;

    db.keyspace("default", KeyspaceCreateOptions::default)?
        .insert("a", "1", Buffer)?;

    // NOTE: Nothing to compact (an empty keyspace), but it still takes a seqno
    let before = db.next_seqno();

    db.keyspace("empty", KeyspaceCreateOptions::default)?
        .compact()?;

    assert!(
        db.next_seqno() > before,
        "the no-op compaction should take a seqno",
    );

    let mut engine = Backup::open(backup_folder.path())?;
    let id = engine.create_backup(&db, BackupMode::KeepLogs)?;
    engine.restore_backup(id, restore_folder.path())?;

    let restored = Database::builder(restore_folder.path())
        .worker_threads_unchecked(0)
        .open()?;

    assert_eq!(db.next_seqno(), restored.next_seqno());
    assert_eq!(db.visible_seqno(), restored.visible_seqno());

    Ok(())
}
