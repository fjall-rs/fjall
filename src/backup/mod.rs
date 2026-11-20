// Copyright (c) 2026-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

mod create;
mod file;
mod info;
mod remove;
mod restore;

pub use create::BackupMode;
pub use info::{BackupId, BackupInfo};

use crate::{
    backup::file::{PRIVATE_FOLDER, SHARED_FOLDER, VERSION_FILE},
    file::{fsync_directory, BLOBS_FOLDER, LOCK_FILE, TABLES_FOLDER},
    file_resolver::KeyspaceFileResolver,
    keyspace::InternalKeyspaceId,
    legacy::watermark::LegacyWatermark,
    locked_file::LockedFileGuard,
    manifest::{Manifest, CURRENT_VERSION_MARKER},
};
use lsm_tree::FileResolver;
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
};

/// Different from a database's version marker (`FJL`), so neither is mistaken for the other
const VERSION_MAGIC: [u8; 3] = *b"FJB";

/// Bump when the layout or the naming of files changes
const VERSION: u8 = 4;

/// Checks the backup folder's format version, or writes it for a new folder.
///
/// A folder without a version file is only accepted if it has no backups yet.
fn check_or_write_version(path: &Path) -> crate::Result<()> {
    let version_path = path.join(VERSION_FILE);

    match std::fs::read(&version_path) {
        Ok(bytes) => {
            if bytes.get(0..3) == Some(&VERSION_MAGIC[..]) && bytes.get(3) == Some(&VERSION) {
                return Ok(());
            }

            log::error!(
                "Unknown backup folder version in {}: {bytes:?}",
                version_path.display(),
            );
            Err(crate::Error::InvalidVersion(None))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            let private_folder = path.join(PRIVATE_FOLDER);

            let has_backups = private_folder.try_exists()?
                && std::fs::read_dir(&private_folder)?.next().is_some();

            if has_backups {
                log::error!(
                    "Backup folder {} has backups, but no version file",
                    path.display(),
                );
                return Err(crate::Error::InvalidVersion(None));
            }

            let mut bytes = VERSION_MAGIC.to_vec();
            bytes.push(VERSION);

            crate::file::rewrite_atomic(&version_path, &bytes)?;
            fsync_directory(path)?;

            Ok(())
        }
        Err(e) => Err(e.into()),
    }
}

/// A folder of incremental backups of a database
//
// A backup folder holds many backups:
//
// ```text
// backups/
//   lock                              # held by the Backup
//   version                           # backup folder format version
//   shared/
//     tables/{id}                     # tables, shared by all backups
//     blobs/{id}                      # blob files, shared by all backups
//   private/{backup_id}/              # a backup's manifest (+ `current`) and WAL fragments
// ```
//
// A backup's manifest lists every file it references (keyspace, file ID, checksum),
// and its `current` marker is the commit point: it is written last, so a backup exists
// if and only if it has one.
//
// Shared files have the same names as in the database (migrated v3 files:
// `{keyspace ID}_{id}`, see [`crate::file_resolver`]). That's unique, because a backup folder
// is only ever fed by one database at a time: a restored database takes over the folder,
// and gets file IDs above every file in it.
pub struct Backup {
    path: PathBuf,

    /// Held as long as the engine is open, so only one engine uses the folder
    _lock_file: LockedFileGuard,

    /// Sorted by ID
    backups: Vec<BackupInfo>,

    /// The shared files each backup references
    files: BTreeMap<BackupId, BTreeSet<PathBuf>>,
}

impl Backup {
    /// Opens a backup folder, or creates it if it doesn't exist.
    ///
    /// Leftovers of a backup that crashed before it was committed are removed.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::Locked`] if another backup engine has the folder open.
    ///
    /// Returns [`crate::Error::InvalidVersion`] if the folder has an unknown backup format
    /// version, or has backups but no version file (it wasn't created by a backup engine).
    ///
    /// Returns an error if an I/O error occurs.
    pub fn open(path: impl AsRef<Path>) -> crate::Result<Self> {
        let path = path.as_ref();
        log::debug!("Opening backup folder at {}", path.display());

        std::fs::create_dir_all(path)?;

        let lock_file = LockedFileGuard::create_new(&path.join(LOCK_FILE))?;

        // NOTE: Under the lock, so two engines can't both see a new folder
        check_or_write_version(path)?;

        std::fs::create_dir_all(path.join(PRIVATE_FOLDER))?;
        std::fs::create_dir_all(path.join(SHARED_FOLDER).join(TABLES_FOLDER))?;
        std::fs::create_dir_all(path.join(SHARED_FOLDER).join(BLOBS_FOLDER))?;

        let mut engine = Self {
            path: path.to_path_buf(),
            _lock_file: lock_file,
            backups: Vec::new(),
            files: BTreeMap::new(),
        };

        for (info, files) in engine.load_backups()? {
            engine.files.insert(info.id, files);
            engine.backups.push(info);
        }

        // NOTE: Leftovers of a crashed `create_backup` (or `remove_backup`)
        engine.remove_unreferenced_files()?;

        log::debug!("Opened backup folder with {} backups", engine.backups.len());

        Ok(engine)
    }

    /// Returns all backups, oldest (lowest ID) first.
    #[must_use]
    pub fn backups(&self) -> &[BackupInfo] {
        &self.backups
    }

    /// Reads all committed backups, and removes the ones that crashed before their commit.
    fn load_backups(&self) -> crate::Result<Vec<(BackupInfo, BTreeSet<PathBuf>)>> {
        let private_folder = self.path.join(PRIVATE_FOLDER);

        let mut backups = Vec::new();
        let mut removed_any = false;

        for entry in std::fs::read_dir(&private_folder)? {
            let entry = entry?;

            let Some(id) = entry
                .file_name()
                .to_str()
                .and_then(|name| name.parse::<BackupId>().ok())
            else {
                log::warn!(
                    "Ignoring unknown entry {} in backup folder",
                    entry.path().display(),
                );
                continue;
            };

            if !entry.path().join(CURRENT_VERSION_MARKER).try_exists()? {
                log::info!("Removing backup #{id}, it was never committed");
                std::fs::remove_dir_all(entry.path())?;
                removed_any = true;
                continue;
            }

            backups.push(self.read_backup(id)?);
        }

        if removed_any {
            fsync_directory(&private_folder)?;
        }

        backups.sort_by_key(|(backup, _)| backup.id);

        Ok(backups)
    }

    /// Reads a committed backup from its manifest: its info, and the shared files it references.
    fn read_backup(&self, id: BackupId) -> crate::Result<(BackupInfo, BTreeSet<PathBuf>)> {
        let backup_path = self.private_path(id);

        let timestamp = std::fs::metadata(backup_path.join(CURRENT_VERSION_MARKER))?.modified()?;

        let mut manifest = Manifest::open(&backup_path)?;
        let recovery = manifest.recover()?;

        let mut size_bytes = 0;
        let mut files = BTreeSet::new();

        let mut table_count = 0;
        let mut blob_file_count = 0;

        let mut next_table_id = recovery.next_table_id;
        let mut next_blob_file_id = recovery.next_blob_file_id;

        for (&keyspace_id, version) in &recovery.keyspace_versions {
            let resolver = self.shared_resolver(keyspace_id, recovery.legacy_watermark);

            let tables = version
                .tables()
                .inspect(|table| {
                    table_count += 1;
                    next_table_id = next_table_id.max(table.id() + 1);
                })
                .map(|table| resolver.resolve_table(table.id()));

            let blob_files = version
                .blob_files()
                .iter()
                .inspect(|blob_file| {
                    blob_file_count += 1;
                    next_blob_file_id = next_blob_file_id.max(blob_file.id() + 1);
                })
                .map(|blob_file| resolver.resolve_blob_file(blob_file.id()));

            for file in tables.chain(blob_files) {
                let metadata = std::fs::metadata(&file).inspect_err(|e| {
                    log::error!(
                        "Backup #{id} is corrupt, it references {}: {e:?}",
                        file.display(),
                    );
                })?;

                size_bytes += metadata.len();
                files.insert(file);
            }
        }

        let info = BackupInfo {
            id,
            timestamp,
            seqno: recovery.highest_seqno.unwrap_or_default(),
            size_bytes,
            table_count,
            blob_file_count,
            next_table_id,
            next_blob_file_id,
        };

        Ok((info, files))
    }

    /// Removes the shared files no backup references (anymore), e.g. of a removed backup,
    /// or leftovers of a backup that crashed before it was committed (including `.tmp` files).
    fn remove_unreferenced_files(&self) -> crate::Result<()> {
        let referenced = self.files.values().flatten().collect::<BTreeSet<_>>();

        for folder in [TABLES_FOLDER, BLOBS_FOLDER] {
            let folder = self.path.join(SHARED_FOLDER).join(folder);
            let mut removed_any = false;

            for entry in std::fs::read_dir(&folder)? {
                let entry = entry?;
                let path = entry.path();

                if !referenced.contains(&path) && entry.file_type()?.is_file() {
                    log::debug!("Removing unreferenced {}", path.display());
                    std::fs::remove_file(&path)?;
                    removed_any = true;
                }
            }

            if removed_any {
                fsync_directory(&folder)?;
            }
        }

        Ok(())
    }

    /// The private folder of a backup (its manifest and WAL fragments).
    fn private_path(&self, id: BackupId) -> PathBuf {
        self.path.join(PRIVATE_FOLDER).join(id.to_string())
    }

    /// Resolves a keyspace's tables and blob files to their paths in the shared folder
    /// (same names as in the database).
    fn shared_resolver(
        &self,
        keyspace_id: InternalKeyspaceId,
        legacy: Option<LegacyWatermark>,
    ) -> KeyspaceFileResolver {
        KeyspaceFileResolver {
            db_path: self.path.join(SHARED_FOLDER),
            keyspace_id,
            legacy,
        }
    }
}

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::manifest::DbEditRecord;
    use lsm_tree::{BlobFileRef, Checksum, EditRecord, TableRef, VersionEdit};
    use test_log::test;

    /// Writes a committed backup by hand: a manifest with one table and one blob file.
    ///
    /// Returns the paths of the two shared files (not created).
    fn write_backup(folder: &Path, id: BackupId) -> crate::Result<[PathBuf; 2]> {
        let backup_path = folder.join(PRIVATE_FOLDER).join(id.to_string());
        std::fs::create_dir_all(&backup_path)?;

        let table_checksum = Checksum::from_raw(0xab);
        let blob_checksum = Checksum::from_raw(0xcd);

        {
            let mut manifest = Manifest::create_new(&backup_path)?;

            manifest.append(
                0,
                &VersionEdit::new(vec![
                    EditRecord::AddTables {
                        level: 0,
                        tables: vec![TableRef::new(1, table_checksum)],
                    },
                    EditRecord::AddBlobFiles {
                        blob_files: vec![BlobFileRef::new(2, blob_checksum)],
                    },
                    EditRecord::SetVersionSeqno(42),
                ]),
            )?;

            manifest.place_current_marker()?;
        }

        Ok([
            folder.join(SHARED_FOLDER).join(TABLES_FOLDER).join("1"),
            folder.join(SHARED_FOLDER).join(BLOBS_FOLDER).join("2"),
        ])
    }

    #[test]
    fn backup_engine_open_creates_layout() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;
        let path = folder.path().join("backups");

        let engine = Backup::open(&path)?;
        assert_eq!(0, engine.backups().len());

        assert!(path.join(LOCK_FILE).is_file());
        assert!(path.join(PRIVATE_FOLDER).is_dir());
        assert!(path.join(SHARED_FOLDER).join(TABLES_FOLDER).is_dir());
        assert!(path.join(SHARED_FOLDER).join(BLOBS_FOLDER).is_dir());

        // NOTE: Only one engine per folder
        assert!(matches!(Backup::open(&path), Err(crate::Error::Locked),));

        drop(engine);
        let _engine = Backup::open(&path)?;

        Ok(())
    }

    #[test]
    fn backup_engine_open_removes_leftovers() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;
        let path = folder.path();

        // NOTE: Creates the folder and its version file
        drop(Backup::open(path)?);

        let crashed = path.join(PRIVATE_FOLDER).join("7");
        let unknown = path.join(PRIVATE_FOLDER).join("notes");
        let tmp = path.join(SHARED_FOLDER).join(TABLES_FOLDER).join("1_x.tmp");

        std::fs::create_dir_all(&crashed)?;
        std::fs::create_dir_all(&unknown)?;
        std::fs::create_dir_all(path.join(SHARED_FOLDER).join(TABLES_FOLDER))?;
        std::fs::write(&tmp, b"partial copy")?;

        let engine = Backup::open(path)?;
        assert_eq!(0, engine.backups().len());

        assert!(!crashed.exists(), "uncommitted backup should be removed");
        assert!(!tmp.exists(), "tmp file should be removed");
        assert!(unknown.exists(), "unknown entries should be left alone");

        Ok(())
    }

    #[test]
    fn backup_engine_open_lists_committed_backup() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;
        let path = folder.path();

        // NOTE: Creates the folder and its version file
        drop(Backup::open(path)?);

        for (file, contents) in write_backup(path, 3)?
            .iter()
            .zip([&b"table"[..], &b"blob file"[..]])
        {
            std::fs::create_dir_all(file.parent().expect("should have parent"))?;
            std::fs::write(file, contents)?;
        }

        let engine = Backup::open(path)?;

        let [backup] = engine.backups() else {
            panic!("should have exactly one backup");
        };

        assert_eq!(3, backup.id);
        assert_eq!(42, backup.seqno);
        assert_eq!(1, backup.table_count);
        assert_eq!(1, backup.blob_file_count);
        assert_eq!(2, backup.file_count());
        assert_eq!(
            ("table".len() + "blob file".len()) as u64,
            backup.size_bytes,
        );

        Ok(())
    }

    #[test]
    fn backup_engine_open_fails_on_missing_shared_file() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;
        let path = folder.path();

        // NOTE: Creates the folder and its version file
        drop(Backup::open(path)?);

        // NOTE: The shared files are never created
        write_backup(path, 3)?;

        assert!(Backup::open(path).is_err());

        Ok(())
    }

    #[test]
    fn backup_engine_writes_and_checks_version() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;
        let path = folder.path();

        drop(Backup::open(path)?);
        assert_eq!(b"FJB\x04", &std::fs::read(path.join(VERSION_FILE))?[..]);

        // NOTE: Reopening accepts its own version
        drop(Backup::open(path)?);

        std::fs::write(path.join(VERSION_FILE), b"FJB\x63")?;
        assert!(matches!(
            Backup::open(path),
            Err(crate::Error::InvalidVersion(None)),
        ));

        Ok(())
    }

    /// A folder with backups but no version file wasn't created by a backup engine
    #[test]
    fn backup_engine_rejects_unversioned_folder_with_backups() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;
        let path = folder.path();

        std::fs::create_dir_all(path.join(PRIVATE_FOLDER).join("1"))?;

        assert!(matches!(
            Backup::open(path),
            Err(crate::Error::InvalidVersion(None)),
        ));
        assert!(
            !path.join(VERSION_FILE).exists(),
            "should not claim a foreign folder",
        );

        Ok(())
    }

    /// A database folder is not accepted as a backup folder
    #[test]
    fn backup_engine_rejects_database_folder() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;
        let path = folder.path();

        let mut marker = vec![];
        crate::version::FormatVersion::V4.write_file_header(&mut marker)?;
        std::fs::write(path.join(VERSION_FILE), marker)?;

        assert!(matches!(
            Backup::open(path),
            Err(crate::Error::InvalidVersion(None)),
        ));

        Ok(())
    }

    /// Migrated v3 files (below the legacy watermark) are stored under their legacy names
    #[test]
    fn backup_engine_resolves_legacy_file_names() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;
        let path = folder.path();

        // NOTE: Creates the folder and its version file
        drop(Backup::open(path)?);

        let backup_path = path.join(PRIVATE_FOLDER).join("1");
        std::fs::create_dir_all(&backup_path)?;

        {
            let mut manifest = Manifest::create_new(&backup_path)?;

            manifest.append_db_edits(&[DbEditRecord::SetLegacyWatermark {
                tables: 10,
                blob_files: 10,
            }])?;

            manifest.append(
                5,
                &VersionEdit::new(vec![EditRecord::AddTables {
                    level: 0,
                    // NOTE: 3 is a migrated v3 table, 11 a v4 one
                    tables: vec![
                        TableRef::new(3, Checksum::from_raw(1)),
                        TableRef::new(11, Checksum::from_raw(2)),
                    ],
                }]),
            )?;

            manifest.place_current_marker()?;
        }

        let tables = path.join(SHARED_FOLDER).join(TABLES_FOLDER);
        std::fs::write(tables.join("5_3"), b"v3")?;
        std::fs::write(tables.join("11"), b"v4")?;

        let engine = Backup::open(path)?;
        let [backup] = engine.backups() else {
            panic!("should have exactly one backup");
        };

        assert_eq!(2, backup.table_count);
        assert_eq!(0, backup.blob_file_count);
        assert_eq!(2, backup.file_count());

        Ok(())
    }
}
