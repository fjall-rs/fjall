use super::*;
use crate::{
    file::TABLES_FOLDER, BackupMode, Database, DurabilityLevel::Buffer, KeyspaceCreateOptions,
};
use test_log::test;

#[test]
fn remove_backup_removes_only_its_own_files() -> crate::Result<()> {
    let db_folder = tempfile::tempdir()?;
    let backup_folder = tempfile::tempdir()?;
    let backups = backup_folder.path();

    let shared_tables = backups
        .join(super::super::SHARED_FOLDER)
        .join(TABLES_FOLDER);

    let db = Database::builder(db_folder.path())
        .worker_threads_unchecked(0)
        .open()?;

    let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;

    {
        let mut backup = Backup::open(backups)?;

        // NOTE: The first two backups share the table of "a", the third only has the compacted one
        tree.insert("a", "1", Buffer)?;
        tree.flush(true)?;
        backup.create_backup(&db, BackupMode::KeepLogs)?;

        tree.insert("b", "2", Buffer)?;
        tree.flush(true)?;
        backup.create_backup(&db, BackupMode::KeepLogs)?;

        tree.major_compact()?;
        backup.create_backup(&db, BackupMode::KeepLogs)?;

        assert_eq!(3, std::fs::read_dir(&shared_tables)?.count());

        // NOTE: Its table is still referenced by the second backup
        backup.remove_backup(1)?;
        assert_eq!(3, std::fs::read_dir(&shared_tables)?.count());
        assert!(!backups.join(PRIVATE_FOLDER).join("1").exists());

        // NOTE: Now the tables of "a" and "b" are unreferenced
        backup.remove_backup(2)?;
        assert_eq!(1, std::fs::read_dir(&shared_tables)?.count());
        assert_eq!(
            vec![3],
            backup.backups().iter().map(|b| b.id).collect::<Vec<_>>(),
        );
    }

    let engine = Backup::open(backups)?;
    assert_eq!(1, engine.backups().len());
    assert_eq!(1, std::fs::read_dir(&shared_tables)?.count());

    Ok(())
}

#[test]
fn remove_backup_unknown_id_fails() -> crate::Result<()> {
    let backup_folder = tempfile::tempdir()?;

    let mut backup = Backup::open(backup_folder.path())?;
    assert!(backup.remove_backup(1).is_err());

    Ok(())
}

#[test]
fn prune_old_backups_keeps_newest() -> crate::Result<()> {
    let db_folder = tempfile::tempdir()?;
    let backup_folder = tempfile::tempdir()?;
    let backups = backup_folder.path();

    let shared_tables = backups
        .join(super::super::SHARED_FOLDER)
        .join(TABLES_FOLDER);

    let db = Database::builder(db_folder.path())
        .worker_threads_unchecked(0)
        .open()?;

    let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;

    let mut backup = Backup::open(backups)?;

    tree.insert("a", "1", Buffer)?;
    tree.flush(true)?;
    backup.create_backup(&db, BackupMode::KeepLogs)?;

    tree.insert("b", "2", Buffer)?;
    tree.flush(true)?;
    backup.create_backup(&db, BackupMode::KeepLogs)?;

    tree.major_compact()?;
    backup.create_backup(&db, BackupMode::KeepLogs)?;

    backup.prune_old_backups(5)?;
    assert_eq!(3, backup.backups().len(), "nothing to prune");

    // NOTE: Only the compacted table is left
    backup.prune_old_backups(1)?;
    assert_eq!(
        vec![3],
        backup.backups().iter().map(|b| b.id).collect::<Vec<_>>(),
    );
    assert_eq!(1, std::fs::read_dir(&shared_tables)?.count());

    backup.prune_old_backups(0)?;
    assert_eq!(0, backup.backups().len());
    assert_eq!(0, std::fs::read_dir(&shared_tables)?.count());

    Ok(())
}

/// A removal that was interrupted after removing the commit point is finished by `open`
#[test]
fn interrupted_remove_is_cleaned_up_on_open() -> crate::Result<()> {
    let db_folder = tempfile::tempdir()?;
    let backup_folder = tempfile::tempdir()?;
    let backups = backup_folder.path();

    let db = Database::builder(db_folder.path())
        .worker_threads_unchecked(0)
        .open()?;

    let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
    tree.insert("a", "1", Buffer)?;
    tree.flush(true)?;

    {
        let mut backup = Backup::open(backups)?;
        backup.create_backup(&db, BackupMode::KeepLogs)?;
    }

    // NOTE: Like a crash right after removing the commit point
    std::fs::remove_file(
        backups
            .join(PRIVATE_FOLDER)
            .join("1")
            .join(CURRENT_VERSION_MARKER),
    )?;

    let backup = Backup::open(backups)?;

    assert_eq!(0, backup.backups().len());
    assert!(!backups.join(PRIVATE_FOLDER).join("1").exists());
    assert_eq!(
        0,
        std::fs::read_dir(
            backups
                .join(super::super::SHARED_FOLDER)
                .join(TABLES_FOLDER)
        )?
        .count()
    );

    Ok(())
}
