use crate::{
    keyspace::InternalKeyspaceId,
    legacy::watermark::LegacyWatermark,
    manifest::{DbEditRecord, Manifest},
    recovery::{KeyspaceInfo, MetaKeyspaceInfo},
    HashMap,
};
use byteorder::ReadBytesExt;
use lsm_tree::SeqNo;
use varint_rs::VarintReader;

pub struct RecoveryResult {
    pub keyspace_info: MetaKeyspaceInfo,
    pub keyspace_versions: HashMap<InternalKeyspaceId, lsm_tree::RawVersionData>,
    /// `None` if the manifest has no versions yet
    pub highest_seqno: Option<SeqNo>,

    /// Set if the DB was migrated from v3
    pub legacy_watermark: Option<LegacyWatermark>,

    /// Above every keyspace ID ever handed out (including deleted keyspaces)
    pub next_keyspace_id: InternalKeyspaceId,

    /// Lower bounds for the next table and blob file IDs, see [`DbEditRecord::SetFileIdCounters`]
    pub next_table_id: u64,
    pub next_blob_file_id: u64,

    /// Lower bound for the next seqno, see [`DbEditRecord::SetSeqnoCounter`]
    pub next_seqno: SeqNo,
}

/// Raises the next table and blob file IDs above every file the version edit adds.
fn raise_file_id_counters(
    version_edit: &lsm_tree::VersionEdit,
    next_table_id: &mut u64,
    next_blob_file_id: &mut u64,
) {
    for record in version_edit.iter() {
        match record {
            lsm_tree::EditRecord::AddTables { tables, .. } => {
                for table in tables {
                    *next_table_id = (*next_table_id).max(table.id() + 1);
                }
            }
            lsm_tree::EditRecord::AddBlobFiles { blob_files } => {
                for blob_file in blob_files {
                    *next_blob_file_id = (*next_blob_file_id).max(blob_file.id() + 1);
                }
            }
            _ => {}
        }
    }
}

#[expect(clippy::too_many_lines)]
pub fn recover_db_manifest(manifest: &mut Manifest) -> crate::Result<RecoveryResult> {
    use lsm_tree::coding::Decode;

    let mut highest_seqno: Option<SeqNo> = None;
    let mut keyspace_info = MetaKeyspaceInfo::default();
    let mut keyspace_versions = HashMap::<_, lsm_tree::RawVersionData>::default();
    let mut legacy_watermark = None;
    let mut next_keyspace_id: InternalKeyspaceId = 0;
    let mut next_table_id = 0;
    let mut next_blob_file_id = 0;
    let mut next_seqno = 0;

    // NOTE: A compaction that was still running can append version edits after the deletion
    let mut deleted_keyspaces = std::collections::HashSet::new();

    // NOTE: Only the current fragment is left at this point (see `Manifest::open`),
    // and it contains the full state on its own
    let mut recovery = manifest.inner.recover()?;

    for wal_record in recovery.by_ref() {
        let wal_record = wal_record?;

        let bytes = wal_record.payload();
        let reader = &mut &bytes[..];

        // TODO: remove magic numbers
        let keyspace_id = match reader.read_u8()? {
            0 => None,
            1 => Some(reader.read_u64_varint()?),
            _ => unreachable!(),
        };

        if let Some(keyspace_id) = keyspace_id {
            let version_edit = lsm_tree::VersionEdit::decode_from(reader)?;

            // NOTE: Every file added since the last snapshot is in an add record, even if it is
            // gone by now (compacted away, cleared), so its ID must not be handed out again.
            // Older files are covered by the snapshot's `SetFileIdCounters`.
            //
            // NOTE: Before skipping deleted keyspaces, their files used the same counters
            raise_file_id_counters(&version_edit, &mut next_table_id, &mut next_blob_file_id);

            if deleted_keyspaces.contains(&keyspace_id) {
                log::debug!("Ignoring VersionEdit of deleted keyspace {keyspace_id}");
                continue;
            }

            log::debug!("Applying VersionEdit to keyspace {keyspace_id}: {version_edit:#?}");

            // NOTE: Seqno 0 is skipped: every tree starts with an initial version at seqno 0,
            // which was never handed out (and a manifest snapshot writes it like any version).
            // A real version can only have seqno 0 if it was the very first thing to take a
            // seqno; handing out 0 again then is harmless (nothing was written with it), and
            // a snapshot records the exact next seqno anyway (`SetSeqnoCounter`).
            if let Some(seqno) = version_edit
                .iter()
                .filter_map(|r| match r {
                    lsm_tree::EditRecord::SetVersionSeqno(seqno) if *seqno > 0 => Some(seqno),
                    _ => None,
                })
                .max()
            {
                highest_seqno = highest_seqno.max(Some(*seqno));
            }

            keyspace_versions
                .entry(keyspace_id)
                .and_modify(|v| {
                    v.apply(&version_edit);
                })
                .or_insert_with(|| {
                    let mut state = lsm_tree::RawVersionData::default();
                    state.apply(&version_edit);
                    state
                });
        } else {
            let edit_count = reader.read_u32_varint()?;

            for _ in 0..edit_count {
                let db_edit = DbEditRecord::decode_from(reader)?;
                log::debug!("Applying DB edit: {db_edit:#?}");

                match db_edit {
                    DbEditRecord::CreateKeyspace { id, name } => {
                        next_keyspace_id = next_keyspace_id.max(id + 1);

                        keyspace_info
                            .entry(id)
                            .and_modify(|entry| {
                                entry.id = id;
                                entry.name = name.clone();
                            })
                            .or_insert_with(|| KeyspaceInfo {
                                id,
                                name,
                                config: HashMap::default(),
                            });
                    }
                    DbEditRecord::ConfigurationChanged { id, k, v } => {
                        if let Some(entry) = keyspace_info.get_mut(&id) {
                            entry.config.insert(k.to_string(), v.to_vec());
                        } else {
                            log::warn!(
                                "Ignoring configuration change for unknown keyspace {id}: {k}"
                            );
                        }
                    }
                    DbEditRecord::DeleteKeyspace { id } => {
                        keyspace_info.remove(&id);
                        keyspace_versions.remove(&id);
                        deleted_keyspaces.insert(id);
                    }
                    DbEditRecord::SetKeyspaceIdCounter { next } => {
                        next_keyspace_id = next_keyspace_id.max(next);
                    }
                    DbEditRecord::SetLegacyWatermark { tables, blob_files } => {
                        legacy_watermark = Some(LegacyWatermark { tables, blob_files });
                    }
                    DbEditRecord::SetFileIdCounters { tables, blob_files } => {
                        next_table_id = next_table_id.max(tables);
                        next_blob_file_id = next_blob_file_id.max(blob_files);
                    }
                    DbEditRecord::SetSeqnoCounter { next } => {
                        next_seqno = next_seqno.max(next);
                    }
                }
            }
        }
    }

    // IMPORTANT: Finish recovery, otherwise the manifest writer has a wrong
    // write position & seqno, and new edits are appended incorrectly
    recovery.finish()?;

    Ok(RecoveryResult {
        keyspace_info,
        keyspace_versions,
        highest_seqno,
        legacy_watermark,
        next_keyspace_id,
        next_table_id,
        next_blob_file_id,
        next_seqno,
    })
}
