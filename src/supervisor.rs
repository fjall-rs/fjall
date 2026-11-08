// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use lsm_tree::SequenceNumberCounter;

use crate::{
    db::Keyspaces,
    keyspace::KeyspaceKey,
    ledger::{Ledger, LedgerGuard},
    legacy::watermark::LegacyWatermark,
    manifest::DbEditRecord,
    poison::PoisonSignal,
    snapshot_tracker::SnapshotTracker,
    write_buffer_manager::WriteBufferManager,
    write_pipeline::WritePipeline,
};
use std::sync::{Arc, Mutex, OnceLock, RwLock};

pub struct SupervisorInner {
    pub db_config: crate::Config,

    /// Next keyspace ID to hand out (same counter as the database's)
    pub(crate) keyspace_id_counter: SequenceNumberCounter,

    /// Dictionary of all keyspaces
    #[doc(hidden)]
    pub keyspaces: Arc<RwLock<Keyspaces>>,

    pub(crate) write_buffer_size: WriteBufferManager,

    pub seqno: SequenceNumberCounter,

    pub snapshot_tracker: SnapshotTracker,

    /// The WAL and the DB manifest, see [`Ledger`]
    ///
    /// Shared with the write pipeline, which writes to the WAL under this lock.
    pub(crate) ledger: Arc<Mutex<Ledger>>,

    pub(crate) write_pipeline: WritePipeline,

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

    /// Locks the ledger (the WAL and the DB manifest), see [`Ledger`].
    pub(crate) fn ledger(&self) -> crate::Result<LedgerGuard<'_>> {
        self.ledger.lock().map_err(|_| crate::Error::Poisoned)
    }

    /// Removes sealed WAL fragments whose items are all flushed, see [`crate::wal::Wal::prune`].
    pub(crate) fn prune_wal(&self, ledger: &mut LedgerGuard<'_>) -> crate::Result<()> {
        #[expect(clippy::expect_used)]
        let keyspaces = self.keyspaces.read().expect("lock is poisoned");

        ledger.wal.prune(&keyspaces)
    }

    /// Rotates the WAL if its active fragment is larger than the configured threshold.
    pub(crate) fn maybe_rotate_wal(&self, ledger: &mut LedgerGuard<'_>) -> crate::Result<()> {
        if ledger.wal.active_fragment_size() <= self.db_config.wal_rotation_threshold {
            return Ok(());
        }

        #[expect(clippy::expect_used)]
        let keyspaces = self.keyspaces.read().expect("lock is poisoned");

        log::debug!("Rotating WAL");
        ledger.wal.rotate(&keyspaces)?;
        ledger.wal.prune(&keyspaces)?;

        if ledger.wal.disk_usage()? >= self.db_config.max_journaling_size_in_bytes {
            for keyspace in ledger
                .wal
                .get_keyspaces_blocking_oldest_fragment(&keyspaces)
            {
                log::info!("Rotating {:?} to try to reduce WAL size", keyspace.name,);
                keyspace.request_rotation();
            }
        }

        Ok(())
    }

    /// Rotates the DB manifest if its active fragment is larger than the configured threshold.
    ///
    /// See [`Supervisor::rotate_manifest`].
    pub(crate) fn maybe_rotate_manifest(
        &self,
        ledger: &mut LedgerGuard<'_>,
        poison: &PoisonSignal,
    ) -> crate::Result<()> {
        if ledger.manifest.active_fragment_size() > self.db_config.manifest_rotation_threshold {
            self.rotate_manifest(ledger, poison)?;
        }
        Ok(())
    }

    /// Writes a snapshot of the full DB state into a new manifest fragment,
    /// and removes the previous fragment.
    ///
    /// IMPORTANT: The caller must NOT hold the keyspaces lock.
    ///
    /// Every manifest edit is persisted and installed while holding the ledger lock,
    /// so the snapshot can't miss an edit that was persisted but not installed yet.
    pub(crate) fn rotate_manifest(
        &self,
        ledger: &mut LedgerGuard<'_>,
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

        ledger
            .manifest
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
}
