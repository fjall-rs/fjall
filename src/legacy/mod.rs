// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

pub mod v3;

use crate::{file::fsync_directory, locked_file::LockedFileGuard, Config};
use crate::{
    keyspace::{
        apply_to_base_config, options::ConfigProvider as KeyspaceConfigProvider,
        InternalKeyspaceId, KeyspaceKey,
    },
    manifest::ManifestPersister,
    Database, Keyspace, KeyspaceCreateOptions,
};
use std::sync::Arc;

pub fn migrate_v3_to_v4(config: Config, lock_file: LockedFileGuard) -> crate::Result<Database> {
    log::info!("Migrating v3 database to v4 format");

    // TODO: refactor this to use a hwal method or so
    let existing_v4_wal = config.path.join("0.hwal");
    if existing_v4_wal.try_exists()? {
        std::fs::remove_file(existing_v4_wal)?;
    }

    let existing_manifest = config.path.join("v0");
    if existing_manifest.try_exists()? {
        std::fs::remove_file(existing_manifest)?;
    }

    let current_file = config.path.join("current");
    if current_file.try_exists()? {
        std::fs::remove_file(current_file)?;
    }

    log::debug!("Preparing database");
    let db = Database::prepare_database(config, lock_file, false)?;

    log::trace!("Recovering v3 meta keyspace contents");
    let keyspace_info = crate::legacy::v3::read_meta_keyspace(db.path())?;

    log::info!("Loading {} existing keyspaces", keyspace_info.len());

    #[expect(clippy::expect_used)]
    recover_keyspaces(
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

    fsync_directory(db.path())?;

    log::trace!("Writing DB manifest");
    // TODO: write all version data into manifest

    // TODO: Finish DB manifest
    // TODO: atomically write "current" file with correct contents
    std::fs::File::create(db.path().join("current"))?;

    // IMPORTANT: Do this as final step
    // TODO: atomically Update "version" file to v4

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

// TODO: just pass in KeyspaceInfo instead?
pub fn recover_keyspaces<P: KeyspaceConfigProvider>(
    db: &Database,
    keyspace_ids: &[InternalKeyspaceId],
    get_keyspace_name: impl Fn(InternalKeyspaceId) -> KeyspaceKey,
    get_config_provider: impl Fn(InternalKeyspaceId) -> P,
) -> crate::Result<()> {
    let registry = &db.supervisor.keyspaces;
    let mut registry = registry.write().expect("lock should not be poisoned");

    let mut highest_keyspace_id = 0;

    for &keyspace_id in keyspace_ids {
        let keyspace_name: KeyspaceKey = get_keyspace_name(keyspace_id);
        log::trace!("Recovering keyspace={keyspace_id} ({keyspace_name:?})");

        // TODO: v4 we need a different path here, when migration is done
        let tree_path = db
            .config
            .path
            .join("keyspaces")
            .join(keyspace_id.to_string());

        let tree_version = lsm_tree::legacy::v3::load_manifest(
            &tree_path,
            // TODO: this should not really require cache & FD cache
            // because we are just reading the manifest data;
            // tables should be loaded in a separate step
            &db.config.cache,
            // TODO: -----------v rename to descriptor_cache?
            db.config
                .descriptor_table
                .as_ref()
                .expect("fd cache should exist"),
        )?;

        log::info!("Linking tables and blob files into DB folders");

        for ks in registry.values() {
            v3::link::link_tree_files(db.path(), ks.id, &ks.tree)?;
        }

        let version_set_provider = Arc::new(lsm_tree::IoVersionSetProvider::with_initial_version(
            tree_version,
            Arc::new(ManifestPersister::new(
                db.supervisor.manifest.clone(),
                keyspace_id,
            )),
        ));

        let config_provider = get_config_provider(keyspace_id);
        let mut recovered_config = KeyspaceCreateOptions::from_kvs(keyspace_id, &config_provider)?;

        // Install compaction filter factory if needed
        if let Some(f) = db
            .config
            .compaction_filter_factory_assigner
            .as_ref()
            .and_then(|f| f(&keyspace_name))
        {
            recovered_config = recovered_config.with_compaction_filter_factory(f);
        }

        let base_config = lsm_tree::Config::new(tree_path, version_set_provider.clone())
            .use_descriptor_table(db.config.descriptor_table.clone())
            .use_cache(db.config.cache.clone())
            .with_table_id_counter(db.table_id_counter.clone())
            .with_blob_file_id_counter(db.blob_file_id_counter.clone());

        let base_config = apply_to_base_config(base_config, &recovered_config);

        let tree = base_config.open()?;

        let keyspace = Keyspace::from_database(
            keyspace_id,
            db,
            tree,
            keyspace_name.clone(),
            recovered_config,
            version_set_provider,
        );

        // Add keyspace to dictionary
        registry.insert(keyspace_name.clone(), keyspace.clone());

        highest_keyspace_id = highest_keyspace_id.max(keyspace_id);
    }

    db.keyspace_id_counter.set(highest_keyspace_id + 1);
    log::trace!(
        "Keyspace ID counter set to {}",
        db.keyspace_id_counter.get(),
    );

    log::debug!("Recovered {} keyspaces", registry.len());

    Ok(())
}
