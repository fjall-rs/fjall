// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{keyspace::InternalKeyspaceId, manifest::DbEditRecord};
use byteorder::WriteBytesExt;
use lsm_tree::coding::Encode;
use varint_rs::VarintWriter;

pub fn encode_db_edit(version_edit: &[DbEditRecord]) -> crate::Result<Vec<u8>> {
    let mut buf = vec![];

    buf.write_u8(0x0)?; // "DB" marker
    buf.write_u32_varint(version_edit.len() as u32)?;

    for record in version_edit {
        record.encode_into(&mut buf)?;
    }

    Ok(buf)
}

pub fn encode_keyspace_version_edit(
    keyspace_id: InternalKeyspaceId,
    version_edit: &lsm_tree::VersionEdit,
) -> crate::Result<Vec<u8>> {
    let mut buf = vec![];
    buf.write_u8(0x1)?; // "Keyspace" marker
    buf.write_u64_varint(keyspace_id)?;
    version_edit.encode_into(&mut buf)?;
    Ok(buf)
}
