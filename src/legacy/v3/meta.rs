use crate::{
    keyspace::{
        options::ConfigProvider as KeyspaceConfigProvider, InternalKeyspaceId, KeyspaceKey,
    },
    HashMap,
};
use byteorder::{ReadBytesExt, BE};
use lsm_tree::{AbstractTree, Guard, SeqNo};
use std::{path::Path, sync::Arc};

#[derive(Debug)]
pub struct KeyspaceInfo {
    pub name: KeyspaceKey,
    config: HashMap<String, Vec<u8>>,
}

impl KeyspaceConfigProvider for &KeyspaceInfo {
    fn get_kv_for_config(&self, config_name: &str) -> crate::Result<Option<lsm_tree::UserValue>> {
        Ok(self.config.get(config_name).map(Into::into))
    }
}

pub type MetaKeyspaceInfo = HashMap<InternalKeyspaceId, KeyspaceInfo>;

#[expect(clippy::expect_used)]
pub fn read_meta_keyspace(path: &Path) -> crate::Result<MetaKeyspaceInfo> {
    // Open the LSM-tree at keyspaces/0 (the meta keyspace in v3)
    //
    // Meta keyspace writes never ran through the journal,
    // so no need to look at the journal; we just need to read
    // the V3 tree "current" version (method provided by lsm-tree)
    let path = path.join("keyspaces").join("0");

    let tree_version = lsm_tree::legacy::v3::load_manifest(
        &path,
        // TODO: this should not really require cache & FD cache
        // because we are just reading the manifest data;
        // tables should be loaded in a separate step
        &Arc::new(lsm_tree::Cache::with_capacity_bytes(
            /* 1 MiB */ 1_024 * 1_024 * 1_024,
        )),
        // TODO: -----------v rename to descriptor_cache?
        &Arc::new(lsm_tree::DescriptorTable::new(5)),
    )?;

    let version_provider = Arc::new(lsm_tree::MemoryVersionSetProvider::with_initial_version(
        tree_version,
    ));
    let tree = lsm_tree::Config::new(path, version_provider).open()?;

    let mut result = MetaKeyspaceInfo::default();

    // Read all KVs, and put them into a list
    for g in tree.iter(SeqNo::MAX, None).rev() {
        let (k, v) = g.into_inner()?;
        let kstr = String::from_utf8_lossy(&k);

        if kstr.starts_with('n') {
            #[expect(clippy::indexing_slicing, reason = "we trust the key schema")]
            let mut k = &k[1..];

            let keyspace_id = k.read_u64::<BE>()?;
            let keyspace_name = std::str::from_utf8(&v).expect("keyspace name should be UTF-8");

            result
                .entry(keyspace_id)
                .and_modify(|entry| {
                    entry.name = KeyspaceKey::new(keyspace_name);
                })
                .or_insert_with(|| KeyspaceInfo {
                    name: KeyspaceKey::new(keyspace_name),
                    config: HashMap::default(),
                });
        } else if kstr.starts_with('c') {
            #[expect(clippy::indexing_slicing, reason = "we trust the key schema")]
            let mut k = &k[1..];

            let keyspace_id = k.read_u64::<BE>()?;
            let config_name = std::str::from_utf8(k).expect("keyspace name should be UTF-8");

            if let Some(i) = result.get_mut(&keyspace_id) {
                i.config.insert(config_name.to_string(), v.to_vec());
            }
        } else {
            panic!("invalid meta keyspace entry");
        }
    }

    Ok(result)
}
