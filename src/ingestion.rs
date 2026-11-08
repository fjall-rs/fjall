// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{worker_pool::WorkerMessage, Keyspace};
use lsm_tree::{AnyIngestion, UserKey, UserValue};

pub struct Ingestion<'a> {
    keyspace: &'a Keyspace,
    inner: AnyIngestion<'a>,

    /// NOTE: Held exclusively from the start until the ingested tables are installed,
    /// so no compaction (or drop range, clear, other ingestion) runs in between
    _compaction_lock: std::sync::RwLockReadGuard<'a, ()>,
}

impl<'a> Ingestion<'a> {
    pub fn new(keyspace: &'a Keyspace) -> crate::Result<Self> {
        // IMPORTANT: Take it before `finish` takes the ledger lock
        // otherwise a compaction that waits for the ledger lock
        // (holding the compaction lock) deadlocks with the ingestion
        let compaction_lock = keyspace
            .compaction_lock
            .read()
            .map_err(|_| crate::Error::Poisoned)?;

        // NOTE: The deletion holds the compaction lock exclusively, so this can't race with it
        if keyspace
            .is_deleted
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(crate::Error::KeyspaceDeleted);
        }

        let inner = keyspace.tree.ingestion()?;

        Ok(Self {
            keyspace,
            inner,
            _compaction_lock: compaction_lock,
        })
    }

    pub fn write<K: Into<UserKey>, V: Into<UserValue>>(
        &mut self,
        key: K,
        value: V,
    ) -> crate::Result<()> {
        self.inner.write(key, value).map_err(Into::into)
    }

    pub fn write_tombstone<K: Into<UserKey>>(&mut self, key: K) -> crate::Result<()> {
        self.inner.write_tombstone(key).map_err(Into::into)
    }

    #[doc(hidden)]
    pub fn write_weak_tombstone<K: Into<UserKey>>(&mut self, key: K) -> crate::Result<()> {
        self.inner.write_weak_tombstone(key).map_err(Into::into)
    }

    /// Commits the ingested data.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn finish(self) -> crate::Result<()> {
        let ks = self.keyspace;

        // IMPORTANT: Hold the ledger lock for the whole finish
        //
        // Otherwise, an older concurrent write ends up in the memtable after the ingestion,
        // and shadows the newer ingested item:
        //
        // write         ingest
        // lock WAL
        // |
        // next seqno=1
        // |
        // --------------finish
        //                 flush
        //                 seqno=2
        //                 register
        //                 |
        // -----------------
        // |
        // insert seqno=1
        let mut ledger = ks.supervisor.ledger()?;

        if ks.is_poisoned.is_poisoned() {
            return Err(crate::Error::Poisoned);
        }

        // NOTE: The flush callback has to return an lsm-tree error, so keep fjall's error aside
        let mut flush_error = None;

        // NOTE: lsm-tree takes the flush lock (lock order: ledger lock, flush lock)
        let result = self.inner.finish(
            || {
                ks.flush_locked(&mut ledger).map_err(|e| {
                    flush_error = Some(e);
                    lsm_tree::Error::Unrecoverable
                })
            },
            || ks.supervisor.seqno.next(),
        );

        if let Some(e) = flush_error {
            return Err(e);
        }

        let Some((flush_result, global_seqno)) = result? else {
            return Ok(());
        };

        // NOTE: The version gets the same seqno as the ingested tables
        ks.version_set_provider
            .register_flush(&mut ledger.persister(ks.id), flush_result, global_seqno)
            .inspect_err(|e| {
                log::error!("Installing a new version failed, which is a FATAL failure: {e:?}");
                ks.is_poisoned.poison();
            })?;

        ks.supervisor.snapshot_tracker.publish(global_seqno);
        ks.prune_versions();

        ks.supervisor
            .maybe_rotate_manifest(&mut ledger, &ks.is_poisoned)?;

        // NOTE: The ingestion flushed the memtables first,
        // so WAL fragments may not be needed anymore
        ks.supervisor.prune_wal(&mut ledger)?;

        drop(ledger);

        // NOTE: Ingested tables are added to L0, so maybe compact
        ks.worker_messager
            .try_send(WorkerMessage::Compact(ks.clone()))
            .ok();

        Ok(())
    }
}
