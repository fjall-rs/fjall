// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    file::{BLOBS_FOLDER, TABLES_FOLDER},
    keyspace::InternalKeyspaceId,
    legacy::watermark::LegacyWatermark,
};
use std::path::PathBuf;

/// File name of a migrated v3 file: `<keyspace ID>_<file ID>`.
///
/// v3 trees numbered their files per tree, so the keyspace ID keeps them apart.
pub fn legacy_file_name(keyspace_id: InternalKeyspaceId, id: u64) -> String {
    format!("{keyspace_id}_{id}")
}

/// Maps a keyspace's table and blob file IDs to their paths in the DB-level folders.
///
/// Files written by v4 are `<db>/tables/<id>` and `<db>/blobs/<id>`,
/// migrated v3 files are `<db>/tables/<keyspace ID>_<id>` and `<db>/blobs/<keyspace ID>_<id>`.
pub struct KeyspaceFileResolver {
    pub db_path: PathBuf,
    pub keyspace_id: InternalKeyspaceId,
    pub legacy: Option<LegacyWatermark>,
}

impl KeyspaceFileResolver {
    fn resolve(&self, folder: &str, id: u64, watermark: Option<u64>) -> PathBuf {
        let file_name = if watermark.is_some_and(|watermark| id < watermark) {
            legacy_file_name(self.keyspace_id, id)
        } else {
            id.to_string()
        };

        self.db_path.join(folder).join(file_name)
    }
}

impl lsm_tree::FileResolver for KeyspaceFileResolver {
    fn resolve_table(&self, id: lsm_tree::TableId) -> PathBuf {
        self.resolve(TABLES_FOLDER, id, self.legacy.map(|w| w.tables))
    }

    fn resolve_blob_file(&self, id: u64) -> PathBuf {
        self.resolve(BLOBS_FOLDER, id, self.legacy.map(|w| w.blob_files))
    }
}
