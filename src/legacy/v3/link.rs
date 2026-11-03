// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    file::{fsync_directory, BLOBS_FOLDER, TABLES_FOLDER},
    keyspace::InternalKeyspaceId,
};
use std::path::Path;

/// Hard links `<src_folder>/<id>` to `<dst_folder>/<keyspace_id>_<id>`.
fn link_file(
    src_folder: &Path,
    dst_folder: &Path,
    keyspace_id: InternalKeyspaceId,
    id: u64,
) -> std::io::Result<()> {
    let src = src_folder.join(id.to_string());
    let dst = dst_folder.join(crate::file_resolver::legacy_file_name(keyspace_id, id));

    log::trace!("Linking {} -> {}", src.display(), dst.display());

    // NOTE: May exist if a previous migration attempt crashed
    if dst.try_exists()? {
        std::fs::remove_file(&dst)?;
    }

    std::fs::hard_link(&src, &dst)
}

/// Hard links all tables and blob files of a v3 tree into the DB-level folders.
///
/// v3 trees number their files per tree, so IDs collide between keyspaces.
/// The files keep their IDs (they are stored inside the files), and get the keyspace ID
/// as prefix instead: `[db]/tables/<keyspace>_<table>`, `[db]/blobs/<keyspace>_<blob file>`.
///
/// New files get IDs from the DB-wide counters, which are raised above all
/// legacy IDs when the trees are opened, so legacy IDs stay free.
pub fn link_tree_files(
    db_path: &Path,
    keyspace_id: InternalKeyspaceId,
    old_tree_path: &Path,
    version: &lsm_tree::Version,
) -> crate::Result<()> {
    let tables_folder = db_path.join(TABLES_FOLDER);
    let blobs_folder = db_path.join(BLOBS_FOLDER);
    std::fs::create_dir_all(&tables_folder)?;
    std::fs::create_dir_all(&blobs_folder)?;

    let legacy_tables_folder = old_tree_path.join(TABLES_FOLDER);

    for table in version.iter_tables() {
        link_file(
            &legacy_tables_folder,
            &tables_folder,
            keyspace_id,
            table.id(),
        )?;
    }

    let legacy_blobs_folder = old_tree_path.join(BLOBS_FOLDER);

    for &blob_file_id in version.blob_files.list_ids() {
        link_file(
            &legacy_blobs_folder,
            &blobs_folder,
            keyspace_id,
            blob_file_id,
        )?;
    }

    fsync_directory(&tables_folder)?;
    fsync_directory(&blobs_folder)?;

    Ok(())
}
