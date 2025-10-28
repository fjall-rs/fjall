use crate::{db::Keyspaces, keyspace::InternalKeyspaceId, Keyspace};
use byteview::StrView;
use lsm_tree::{AbstractTree, AnyTree, SeqNo, SequenceNumberCounter, UserValue};
use std::sync::{Arc, RwLock, RwLockWriteGuard};

pub fn encode_config_key(
    keyspace_id: InternalKeyspaceId,
    name: impl AsRef<[u8]>,
) -> crate::UserKey {
    let mut key: Vec<u8> =
        Vec::with_capacity(std::mem::size_of::<InternalKeyspaceId>() + 1 + name.as_ref().len());
    key.push(b'c');
    key.extend(keyspace_id.to_le_bytes());
    key.push(0);
    #[allow(clippy::string_lit_as_bytes)]
    key.extend(name.as_ref());
    key.into()
}

/// The meta keyspace keeps mappings of keyspace names to their internal IDs and configurations
///
/// The meta keyspace is always keyspace #0.
#[derive(Clone)]
pub struct MetaKeyspace {
    inner: AnyTree,

    /// Dictionary of all keyspaces
    #[doc(hidden)]
    pub keyspaces: Arc<RwLock<Keyspaces>>,

    // TODO: maybe use separate seqnos as not to interfere with other seqno
    seqno_generator: SequenceNumberCounter,
    visible_seqno: SequenceNumberCounter,
}

impl MetaKeyspace {
    pub fn new(
        inner: AnyTree,
        keyspaces: Arc<RwLock<Keyspaces>>,
        seqno_generator: SequenceNumberCounter,
        visible_seqno: SequenceNumberCounter,
    ) -> Self {
        Self {
            inner,
            keyspaces,
            seqno_generator,
            visible_seqno,
        }
    }

    pub(crate) fn get_kv_for_config(
        &self,
        keyspace_id: InternalKeyspaceId,
        name: &str,
    ) -> crate::Result<Option<UserValue>> {
        let key = encode_config_key(keyspace_id, name);
        self.inner.get(key, SeqNo::MAX).map_err(Into::into)
    }

    pub fn create_keyspace(
        &self,
        keyspace_id: InternalKeyspaceId,
        name: &str,
        keyspace: Keyspace,
        mut keyspaces: RwLockWriteGuard<'_, Keyspaces>,
    ) -> crate::Result<()> {
        let mut kvs = keyspace.config.encode_kvs(keyspace_id);

        kvs.push({
            let mut key: Vec<u8> =
                Vec::with_capacity(std::mem::size_of::<InternalKeyspaceId>() + 1);
            key.push(b'n');
            key.extend(keyspace_id.to_le_bytes());

            let value = UserValue::new(name.as_bytes());

            (key.into(), value)
        });

        kvs.sort_by(|(a, _), (b, _)| a.cmp(b));

        #[cfg(debug_assertions)]
        {
            assert!(
                kvs.is_sorted_by_key(|(k, _)| k),
                "config KVs need to be sorted by key",
            );
        }

        self.inner
            .ingest(kvs.into_iter(), &self.seqno_generator, &self.visible_seqno)?;

        keyspaces.insert(name.into(), keyspace);

        Ok(())
    }

    pub fn remove_keyspace(&self, name: &str) -> crate::Result<()> {
        unimplemented!();

        // let mut lock: RwLockWriteGuard<'_, std::collections::HashMap<StrView, Keyspace, xxhash_rust::xxh3::Xxh3Builder>> = self.keyspaces.write().expect("lock is poisoned");

        // let Some(keyspace) = lock.get(name) else {
        //     return Ok(());
        // };

        // let seqno = self.seqno_generator.next();

        // // TODO: remove from LSM-tree ATOMICALLY - name and config
        // // TODO: make sure the ingested stream is sorted
        // let mut ingestion = lsm_tree::Ingestion::new(&self.inner)?.with_seqno(seqno);
        // {
        //     let mut key: Vec<u8> =
        //         Vec::with_capacity(std::mem::size_of::<InternalKeyspaceId>() + 1);
        //     key.push(b'n');
        //     key.extend(keyspace.id.to_le_bytes());
        //     ingestion.write_tombstone(key.into())?;
        // }

        // ingestion.finish()?;

        // self.visible_seqno.fetch_max(seqno + 1);

        // lock.remove(name);

        Ok(())
    }

    pub fn resolve_id(&self, id: InternalKeyspaceId) -> crate::Result<Option<StrView>> {
        let mut key: Vec<u8> = Vec::with_capacity(std::mem::size_of::<InternalKeyspaceId>() + 1);
        key.push(b'n');
        key.extend(id.to_le_bytes());

        Ok(self
            .inner
            .get(key, SeqNo::MAX)?
            .map(|v| std::str::from_utf8(&v).expect("should be utf-8").into()))
    }

    pub fn keyspace_exists(&self, name: &str) -> bool {
        self.keyspaces
            .read()
            .expect("lock is poisoned")
            .contains_key(name)
    }

    // pub fn get_by_name(&self, name: &str) -> Option<Keyspace> {
    //     self.keyspaces
    //         .read()
    //         .expect("lock is poisoned")
    //         .get(name)
    //         .cloned()
    // }
}
