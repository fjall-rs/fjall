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
    key.extend(keyspace_id.to_be_bytes());
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

    seqno_generator: SequenceNumberCounter,
    visible_seqno: SequenceNumberCounter,
}

impl MetaKeyspace {
    pub(crate) fn new(
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

    #[cfg(test)]
    fn len(&self) -> crate::Result<usize> {
        self.inner.len(SeqNo::MAX, None).map_err(Into::into)
    }

    pub(crate) fn get_kv_for_config(
        &self,
        keyspace_id: InternalKeyspaceId,
        name: &str,
    ) -> crate::Result<Option<UserValue>> {
        let key = encode_config_key(keyspace_id, name);
        self.inner.get(key, SeqNo::MAX).map_err(Into::into)
    }

    fn maintenance(&self) -> crate::Result<()> {
        self.inner
            .compact(
                Arc::new(
                    crate::compaction::Leveled::default()
                        .with_l0_threshold(2)
                        .with_level_ratio_policy(vec![20.0]),
                ),
                0,
            )
            .map_err(Into::into)
    }

    pub(crate) fn create_keyspace(
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
            key.extend(keyspace_id.to_be_bytes());

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

        let seqno = self.seqno_generator.next();

        let mut ingestion = self.inner.ingestion()?.with_seqno(seqno);

        for kv in kvs {
            ingestion.write(kv.0, kv.1)?;
        }

        ingestion.finish()?;

        self.visible_seqno.fetch_max(seqno + 1);

        keyspaces.insert(name.into(), keyspace);

        self.maintenance()
            .inspect_err(|e| {
                log::warn!("Meta keyspace maintenance failed: {e:?}");
            })
            .ok();

        Ok(())
    }

    pub(crate) fn remove_keyspace(&self, name: &str) -> crate::Result<()> {
        let mut lock = self.keyspaces.write().expect("lock is poisoned");

        let Some(keyspace) = lock.get(name) else {
            return Ok(());
        };

        let seqno = self.seqno_generator.next();

        let mut ingestion = self.inner.ingestion()?.with_seqno(seqno);
        {
            // Remove all config KVs
            let pfx: Vec<u8> = {
                let mut v = vec![];
                v.push(b'c');
                v.extend(keyspace.id.to_be_bytes());
                v
            };

            for config_kv in self.inner.prefix(pfx, SeqNo::MAX, None) {
                use lsm_tree::Guard;

                let key = config_kv.key()?;
                ingestion.write_tombstone(key)?;
            }

            // Remove ID -> name mapping
            let mut key: Vec<u8> =
                Vec::with_capacity(std::mem::size_of::<InternalKeyspaceId>() + 1);
            key.push(b'n');
            key.extend(keyspace.id.to_be_bytes());
            ingestion.write_tombstone(key)?;
        }
        ingestion.finish()?;

        self.visible_seqno.fetch_max(seqno + 1);

        lock.remove(name);

        self.maintenance()
            .inspect_err(|e| {
                log::warn!("Meta keyspace maintenance failed: {e:?}");
            })
            .ok();

        Ok(())
    }

    pub(crate) fn resolve_id(&self, id: InternalKeyspaceId) -> crate::Result<Option<StrView>> {
        #[expect(unsafe_code, clippy::indexing_slicing)]
        let key = {
            let mut builder = unsafe {
                lsm_tree::Slice::builder_unzeroed(std::mem::size_of::<InternalKeyspaceId>() + 1)
            };
            builder[0] = b'n';
            builder[1..].copy_from_slice(&id.to_be_bytes());
            builder.freeze()
        };

        Ok(self.inner.get(key, SeqNo::MAX)?.map(|v| {
            #[expect(clippy::expect_used, reason = "keyspace open only accepts &str")]
            let name = std::str::from_utf8(&v).expect("should be utf-8");

            name.into()
        }))
    }

    pub(crate) fn keyspace_exists(&self, name: &str) -> bool {
        self.keyspaces
            .read()
            .expect("lock is poisoned")
            .contains_key(name)
    }
}

#[cfg(test)]
mod tests {
    use crate::{Database, Guard, KeyspaceCreateOptions, Readable, SingleWriterTxDatabase};
    use test_log::test;

    const ITEM_COUNT: usize = 10;

    #[test]
    fn keyspace_delete() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;

        let path;

        {
            let db = Database::builder(&folder).open()?;

            assert_eq!(0, db.meta_keyspace.len()?);

            let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
            path = tree.path().to_path_buf();

            assert!(path.try_exists()?);
            assert!(db.meta_keyspace.len()? > 0);

            for x in 0..ITEM_COUNT as u64 {
                let key = x.to_be_bytes();
                let value = nanoid::nanoid!();
                tree.insert(key, value.as_bytes())?;
            }

            for x in 0..ITEM_COUNT as u64 {
                let key: [u8; 8] = (x + ITEM_COUNT as u64).to_be_bytes();
                let value = nanoid::nanoid!();
                tree.insert(key, value.as_bytes())?;
            }

            assert_eq!(tree.len()?, ITEM_COUNT * 2);
            assert_eq!(tree.iter().flat_map(Guard::key).count(), ITEM_COUNT * 2);
            assert_eq!(
                tree.iter().rev().flat_map(Guard::key).count(),
                ITEM_COUNT * 2,
            );
        }

        for _ in 0..10 {
            let db = Database::builder(&folder).open()?;

            let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;

            assert!(path.try_exists()?);
            assert!(db.meta_keyspace.len()? > 0);

            assert_eq!(tree.len()?, ITEM_COUNT * 2);
            assert_eq!(tree.iter().flat_map(Guard::key).count(), ITEM_COUNT * 2);
            assert_eq!(
                tree.iter().rev().flat_map(Guard::key).count(),
                ITEM_COUNT * 2,
            );
        }

        {
            let db = Database::builder(&folder).open()?;

            let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
            assert!(path.try_exists()?);
            assert!(db.meta_keyspace.len()? > 0);

            db.delete_keyspace(tree)?;
            assert!(!path.try_exists()?);
            assert_eq!(0, db.meta_keyspace.len()?);
        }

        {
            let db = Database::builder(&folder).open()?;
            assert!(!path.try_exists()?);
            assert_eq!(0, db.meta_keyspace.len()?);
        }

        Ok(())
    }

    #[test]
    fn tx_keyspace_delete() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;

        let path;

        {
            let db = SingleWriterTxDatabase::builder(&folder).open()?;

            let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
            path = tree.path();

            assert!(path.try_exists()?);

            for x in 0..ITEM_COUNT as u64 {
                let key = x.to_be_bytes();
                let value = nanoid::nanoid!();
                tree.insert(key, value.as_bytes())?;
            }

            for x in 0..ITEM_COUNT as u64 {
                let key: [u8; 8] = (x + ITEM_COUNT as u64).to_be_bytes();
                let value = nanoid::nanoid!();
                tree.insert(key, value.as_bytes())?;
            }

            assert_eq!(db.read_tx().len(&tree)?, ITEM_COUNT * 2);
            assert_eq!(
                db.read_tx().iter(&tree).flat_map(Guard::key).count(),
                ITEM_COUNT * 2,
            );
            assert_eq!(
                db.read_tx().iter(&tree).rev().flat_map(Guard::key).count(),
                ITEM_COUNT * 2,
            );
        }

        for _ in 0..5 {
            let db = SingleWriterTxDatabase::builder(&folder).open()?;

            let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;

            assert_eq!(db.read_tx().len(&tree)?, ITEM_COUNT * 2);
            assert_eq!(
                db.read_tx().iter(&tree).flat_map(Guard::key).count(),
                ITEM_COUNT * 2,
            );
            assert_eq!(
                db.read_tx().iter(&tree).rev().flat_map(Guard::key).count(),
                ITEM_COUNT * 2,
            );

            assert!(path.try_exists()?);
        }

        {
            let db = SingleWriterTxDatabase::builder(&folder).open()?;

            {
                let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;

                assert!(path.try_exists()?);

                db.inner().delete_keyspace(tree.inner().clone())?;
            }

            assert!(!path.try_exists()?);
        }

        {
            let _db = Database::builder(&folder).open()?;
            assert!(!path.try_exists()?);
        }

        Ok(())
    }

    #[test]
    fn keyspace_delete_and_reopening_behavior() -> crate::Result<()> {
        let keyspace_name = "default";
        let folder = tempfile::tempdir()?;

        let keyspace_exists = |id: u64| -> std::io::Result<bool> {
            folder
                .path()
                .join("keyspaces")
                .join(id.to_string())
                .try_exists()
        };

        let db = Database::builder(&folder).open()?;
        assert!(!keyspace_exists(1)?);

        let keyspace = db.keyspace(keyspace_name, KeyspaceCreateOptions::default)?;
        assert!(keyspace_exists(1)?);

        db.delete_keyspace(keyspace)?;
        assert!(!keyspace_exists(1)?);

        assert!(db
            .keyspace("default", KeyspaceCreateOptions::default)
            .is_ok());
        assert!(keyspace_exists(2)?);

        Ok(())
    }
}
