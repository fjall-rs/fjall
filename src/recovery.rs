// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    keyspace::{
        apply_to_base_config, options::ConfigProvider as KeyspaceConfigProvider,
        InternalKeyspaceId, KeyspaceKey,
    },
    Database, Keyspace, KeyspaceCreateOptions,
};
use std::sync::Arc;

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

        // TODO: need persisting provider
        let version_set_provider = Arc::new(
            lsm_tree::MemoryVersionSetProvider::with_initial_version(tree_version),
        );

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
            .use_cache(db.config.cache.clone());

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
