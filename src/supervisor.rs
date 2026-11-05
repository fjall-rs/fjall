// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use lsm_tree::SequenceNumberCounter;

use crate::{
    db::Keyspaces,
    flush::manager::FlushManager,
    keyspace::KeyspaceKey,
    legacy::watermark::LegacyWatermark,
    manifest::{DbEditRecord, Manifest},
    poison::PoisonSignal,
    snapshot_tracker::SnapshotTracker,
    write_buffer_manager::WriteBufferManager,
    write_pipeline::WritePipeline,
};
use std::sync::{Arc, MutexGuard, OnceLock, RwLock};

pub struct SupervisorInner {
    pub db_config: crate::Config,

    /// Next keyspace ID to hand out (same counter as the database's)
    pub(crate) keyspace_id_counter: SequenceNumberCounter,

    /// Dictionary of all keyspaces
    #[doc(hidden)]
    pub keyspaces: Arc<RwLock<Keyspaces>>,

    pub(crate) write_buffer_size: WriteBufferManager,
    pub(crate) flush_manager: FlushManager,

    pub seqno: SequenceNumberCounter,

    pub snapshot_tracker: SnapshotTracker,

    /// Tracks journal size and garbage collects sealed journals when possible
    // pub(crate) journal_manager: Arc<RwLock<JournalManager>>,
    pub(crate) write_pipeline: WritePipeline,

    pub(crate) manifest: Manifest,

    /// Set if the DB was migrated from v3, see [`LegacyWatermark`]
    ///
    /// IMPORTANT: Written into every manifest snapshot, otherwise migrated v3 files
    /// can't be found anymore after a manifest rotation
    pub(crate) legacy_watermark: OnceLock<LegacyWatermark>,
}

#[derive(Clone)]
pub struct Supervisor(Arc<SupervisorInner>);

impl std::ops::Deref for Supervisor {
    type Target = SupervisorInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Supervisor {
    pub fn new(inner: SupervisorInner) -> Self {
        Self(Arc::new(inner))
    }

    /// Rotates the DB manifest if its active fragment is larger than the configured threshold.
    ///
    /// See [`Supervisor::rotate_manifest`].
    pub(crate) fn maybe_rotate_manifest(
        &self,
        wal_lock: &MutexGuard<'_, crate::wal::Wal>,
        poison: &PoisonSignal,
    ) -> crate::Result<()> {
        if self.manifest.active_fragment_size() > self.db_config.manifest_rotation_threshold {
            self.rotate_manifest(wal_lock, poison)?;
        }
        Ok(())
    }

    /// Writes a snapshot of the full DB state into a new manifest fragment,
    /// and removes the previous fragment.
    ///
    /// IMPORTANT: The caller must hold the WAL lock, and must NOT hold the keyspaces lock.
    ///
    /// Every version edit is persisted and installed while holding the WAL lock,
    /// and keyspaces are created while holding the keyspaces write lock, so the snapshot
    /// can't miss an edit that was persisted but not installed yet.
    ///
    /// Lock order: WAL lock, keyspaces lock, manifest lock.
    pub(crate) fn rotate_manifest(
        &self,
        _wal_lock: &MutexGuard<'_, crate::wal::Wal>,
        poison: &PoisonSignal,
    ) -> crate::Result<()> {
        #[expect(clippy::expect_used)]
        let keyspaces = self.keyspaces.read().expect("lock is poisoned");

        // NOTE: Sort by ID, so the snapshot is deterministic
        let mut sorted_keyspaces = keyspaces.values().collect::<Vec<_>>();
        sorted_keyspaces.sort_by_key(|keyspace| keyspace.id);

        let mut db_edits = vec![];

        for keyspace in &sorted_keyspaces {
            db_edits.push(DbEditRecord::CreateKeyspace {
                id: keyspace.id,
                name: keyspace.name.clone(),
            });

            // NOTE: Re-encoded from memory, like when creating the keyspace
            db_edits.extend(
                keyspace
                    .config
                    .encode_kvs(keyspace.id)
                    .into_iter()
                    .map(|(k, v)| DbEditRecord::ConfigurationChanged {
                        id: keyspace.id,
                        k,
                        v,
                    }),
            );
        }

        // IMPORTANT: Deleted keyspaces are not in the snapshot, so keep their IDs from being reused
        db_edits.push(DbEditRecord::SetKeyspaceIdCounter {
            next: self.keyspace_id_counter.get(),
        });

        if let Some(&LegacyWatermark { tables, blob_files }) = self.legacy_watermark.get() {
            db_edits.push(DbEditRecord::SetLegacyWatermark { tables, blob_files });
        }

        let keyspace_edits = sorted_keyspaces.iter().map(|keyspace| {
            (
                keyspace.id,
                lsm_tree::VersionEdit::new(keyspace.version_set_provider.get_edit_records()),
            )
        });

        self.manifest
            .rotate(&db_edits, keyspace_edits)
            .inspect_err(|e| {
                // NOTE: New edits would go to a fragment that `current` might not name
                log::error!("DB manifest rotation failed, which is a FATAL failure: {e:?}");
                poison.poison();
            })
    }

    pub fn keyspace_exists(&self, name: &str) -> bool {
        self.keyspaces
            .read()
            .expect("lock should not be poisoned")
            .contains_key(name)
    }

    pub fn keyspace_count(&self) -> usize {
        self.keyspaces.read().expect("lock is poisoned").len()
    }

    pub fn list_keyspace_names(&self) -> Vec<KeyspaceKey> {
        self.keyspaces
            .read()
            .expect("lock is poisoned")
            .keys()
            .cloned()
            .collect()
    }

    /*pub fn build_seqno_map(
        &self,
        keyspaces: &Keyspaces,
    ) -> Vec<crate::journal::manager::EvictionWatermark> {
        /*use crate::AbstractTree;

        let mut seqnos = Vec::with_capacity(keyspaces.len());

        for keyspace in keyspaces.values() {
            if let Some(lsn) = keyspace.tree.get_highest_memtable_seqno() {
                seqnos.push(crate::journal::manager::EvictionWatermark {
                    lsn,
                    keyspace: keyspace.clone(),
                });
            }
        }*

        seqnos*/

        // TODO:
        todo!()
    }*/
}
