// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    keyspace::{
        options::ConfigProvider as KeyspaceConfigProvider, InternalKeyspaceId, KeyspaceKey,
    },
    HashMap,
};

pub type MetaKeyspaceInfo = HashMap<InternalKeyspaceId, KeyspaceInfo>;

#[derive(Debug)]
pub struct KeyspaceInfo {
    pub id: InternalKeyspaceId,
    pub name: KeyspaceKey,
    pub config: HashMap<String, Vec<u8>>,
}

impl KeyspaceConfigProvider for &KeyspaceInfo {
    fn get_kv_for_config(&self, config_name: &str) -> crate::Result<Option<lsm_tree::UserValue>> {
        Ok(self.config.get(config_name).map(Into::into))
    }
}
