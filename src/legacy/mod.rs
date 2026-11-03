// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

pub mod v3;

use crate::file::{rewrite_atomic, VERSION_MARKER};
use crate::{file::fsync_directory, locked_file::LockedFileGuard, Config};
use crate::{
    keyspace::{apply_to_base_config, KeyspaceKey},
    manifest::{DbEditRecord, ManifestPersister},
    recovery::MetaKeyspaceInfo,
    Database, Keyspace, KeyspaceCreateOptions,
};
use byteview::ByteView;
use lsm_tree::VersionEdit;
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

    recover_keyspaces(&db, &keyspace_info)?;

    // IMPORTANT: Write keyspaces and their versions before replaying the journal,
    // so the flushes below append their edits on top of the full v3 versions
    log::trace!("Writing DB manifest");
    write_keyspaces_to_manifest(&db, &keyspace_info)?;

    log::info!("Replaying v3 journal");
    crate::legacy::v3::journal::do_full_v3_journal_recovery(&db)?;

    {
        use lsm_tree::AbstractTree;

        // IMPORTANT: Continue after the highest seqno of the v3 data (tables + replayed journal),
        // before flushing, so flush seqnos and new writes are above all migrated items
        #[expect(clippy::expect_used)]
        let highest_seqno = db
            .supervisor
            .keyspaces
            .read()
            .expect("lock should not be poisoned")
            .values()
            .filter_map(|ks| ks.tree.get_highest_seqno())
            .max();

        if let Some(highest_seqno) = highest_seqno {
            db.supervisor.seqno.fetch_max(highest_seqno + 1);
            db.supervisor.snapshot_tracker.publish(highest_seqno);
        }
    }

    {
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
    }

    // IMPORTANT: Set the "current" marker
    db.supervisor.manifest.place_current_marker()?;
    fsync_directory(db.path())?;

    // IMPORTANT: Do this as final step
    {
        let mut v = vec![];
        crate::version::FormatVersion::V4.write_file_header(&mut v)?;
        rewrite_atomic(&db.path().join(VERSION_MARKER), &v)?;
    }

    log::debug!("Removing old v3 journal files");
    crate::legacy::v3::journal::nuke_journal(db.path())?;

    log::trace!("Removing old v3 keyspaces folder");
    std::fs::remove_dir_all(db.path().join("keyspaces").join("0"))?;

    Ok(db)
}

/// Writes all recovered v3 keyspaces into the DB manifest:
///
/// - the keyspace names and their configuration, as stored in the v3 meta keyspace
/// - the full current version of every keyspace's tree
fn write_keyspaces_to_manifest(
    db: &Database,
    keyspace_info: &MetaKeyspaceInfo,
) -> crate::Result<()> {
    // NOTE: Sort by ID, so the manifest contents are deterministic
    let mut infos = keyspace_info.values().collect::<Vec<_>>();
    infos.sort_by_key(|info| info.id);

    let mut db_edits = vec![];

    for info in &infos {
        db_edits.push(DbEditRecord::CreateKeyspace {
            id: info.id,
            name: info.name.clone(),
        });

        // NOTE: v3 and v4 store keyspace configuration the same way,
        // so the meta keyspace entries are copied as they are
        let mut config = info.config.iter().collect::<Vec<_>>();
        config.sort_by_key(|(k, _)| *k);

        for (k, v) in config {
            db_edits.push(DbEditRecord::ConfigurationChanged {
                id: info.id,
                k: k.as_str().into(),
                v: ByteView::new(v),
            });
        }
    }

    // NOTE: One DB edit, because faster
    db.supervisor.manifest.append_db_edits(&db_edits)?;

    #[expect(clippy::expect_used)]
    let keyspaces = db
        .supervisor
        .keyspaces
        .read()
        .expect("lock should not be poisoned");

    for info in &infos {
        #[expect(clippy::expect_used)]
        let keyspace = keyspaces
            .get(&info.name)
            .expect("keyspace should have been recovered");

        let edit = VersionEdit::new(keyspace.version_set_provider.get_edit_records());
        db.supervisor.manifest.append(info.id, &edit)?;
    }

    Ok(())
}

/// Loads the trees of all keyspaces listed in the v3 meta keyspace.
fn recover_keyspaces(db: &Database, keyspace_info: &MetaKeyspaceInfo) -> crate::Result<()> {
    let registry = &db.supervisor.keyspaces;
    let mut registry = registry.write().expect("lock should not be poisoned");

    let mut highest_keyspace_id = 0;

    for info in keyspace_info.values() {
        let keyspace_id = info.id;
        let keyspace_name: KeyspaceKey = info.name.clone();
        log::trace!("Recovering keyspace={keyspace_id} ({keyspace_name:?})");

        let old_tree_path = db
            .config
            .path
            .join("keyspaces")
            .join(keyspace_id.to_string());

        let tree_version = lsm_tree::legacy::v3::load_manifest(
            &old_tree_path,
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
        v3::link::link_tree_files(db.path(), keyspace_id, &old_tree_path, &tree_version)?;

        let version_set_provider = Arc::new(lsm_tree::IoVersionSetProvider::with_initial_version(
            tree_version,
            Arc::new(ManifestPersister::new(
                db.supervisor.manifest.clone(),
                keyspace_id,
            )),
        ));

        let mut recovered_config = KeyspaceCreateOptions::from_kvs(keyspace_id, &info)?;

        // Install compaction filter factory if needed
        if let Some(f) = db
            .config
            .compaction_filter_factory_assigner
            .as_ref()
            .and_then(|f| f(&keyspace_name))
        {
            recovered_config = recovered_config.with_compaction_filter_factory(f);
        }

        // TODO: v4 we need a different path here, when migration is done
        let base_config = lsm_tree::Config::new(old_tree_path, version_set_provider.clone())
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
