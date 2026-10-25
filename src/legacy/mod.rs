use crate::{locked_file::LockedFileGuard, Config, Database};

pub mod v3;

pub fn migrate_v3_to_v4(config: Config, lock_file: LockedFileGuard) -> crate::Result<Database> {
    log::info!("Migrating v3 database to v4 format");

    // TODO: refactor this to use a hwal method or so
    let existing_v4_wal = config.path.join("0.hwal");

    if existing_v4_wal.try_exists()? {
        std::fs::remove_file(existing_v4_wal)?;
    }

    log::debug!("Preparing database");
    let db = Database::prepare_database(config, lock_file, false)?;

    log::trace!("Recovering v3 meta keyspace contents");
    let keyspace_info = crate::legacy::v3::read_meta_keyspace(db.path())?;

    log::info!("Loading existing keyspaces");

    #[expect(clippy::expect_used)]
    crate::recovery::recover_keyspaces(
        &db,
        &keyspace_info.keys().copied().collect::<Vec<_>>(),
        |id| {
            keyspace_info
                .get(&id)
                .expect("keyspace should exist")
                .name
                .clone()
        },
        |id: u64| keyspace_info.get(&id).expect("keyspace should exist"),
    )?;

    log::info!("Replaying v3 journal");

    crate::legacy::v3::journal::do_full_v3_journal_recovery(&db)?;

    // NOTE: If flushing fails halfway through, it's fine
    // because the journal entries of that keyspace won't be replayed
    log::info!("Flushing all memtables");

    #[expect(clippy::expect_used)]
    for ks in db
        .supervisor
        .keyspaces
        .read()
        .expect("lock should not be poisoned")
        .values()
    {
        use lsm_tree::AbstractTree;

        crate::flush::worker::run(ks, &db.supervisor, &db.stats)?;

        assert!(
            ks.tree.active_memtable().is_empty(),
            "keyspace's active memtable should be empty",
        );
        assert!(
            ks.sealed_memtable_count() == 0,
            "keyspace should not have sealed memtables",
        );
    }

    // TODO: Finish DB manifest
    // TODO: Update "version" file to v4
    std::fs::File::create(db.path().join("current"))?;

    log::debug!("Removing old v3 files");
    crate::legacy::v3::journal::nuke_journal(db.path())?;

    #[expect(clippy::expect_used)]
    for ks in db
        .supervisor
        .keyspaces
        .read()
        .expect("lock should not be poisoned")
        .values()
    {
        use lsm_tree::AbstractTree;

        let tree_path = &ks.tree.tree_config().path;

        for dirent in std::fs::read_dir(tree_path)? {
            let dirent = dirent?;
            let file_name = dirent.file_name();
            let file_name = file_name.as_encoded_bytes();

            let should_delete = file_name.starts_with(b"v") || file_name == b"current";

            if should_delete {
                log::debug!("Removing old v3 manifest file: {}", dirent.path().display());
                std::fs::remove_file(dirent.path())?;
            }
        }
    }

    log::trace!("Removing old v3 meta keyspace");
    std::fs::remove_dir_all(db.path().join("keyspaces").join("0"))?;

    Ok(db)
}
