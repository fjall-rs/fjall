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
    pub highest_seqno: SeqNo,

    /// Set if the DB was migrated from v3
    pub legacy_watermark: Option<LegacyWatermark>,

    /// Above every keyspace ID ever handed out (including deleted keyspaces)
    pub next_keyspace_id: InternalKeyspaceId,
}

pub fn recover_db_manifest(manifest: &mut Manifest) -> crate::Result<RecoveryResult> {
    use lsm_tree::coding::Decode;

    let mut highest_seqno = SeqNo::default();
    let mut keyspace_info = MetaKeyspaceInfo::default();
    let mut keyspace_versions = HashMap::<_, lsm_tree::RawVersionData>::default();
    let mut legacy_watermark = None;
    let mut next_keyspace_id: InternalKeyspaceId = 0;

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

            if deleted_keyspaces.contains(&keyspace_id) {
                log::debug!("Ignoring VersionEdit of deleted keyspace {keyspace_id}");
                continue;
            }

            log::debug!("Applying VersionEdit to keyspace {keyspace_id}: {version_edit:#?}");

            if let Some(seqno) = version_edit
                .iter()
                .filter_map(|r| match r {
                    lsm_tree::EditRecord::SetVersionSeqno(seqno) => Some(seqno),
                    _ => None,
                })
                .max()
            {
                highest_seqno = highest_seqno.max(*seqno);
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
    })
}
