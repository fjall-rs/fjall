// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use byteorder::{ReadBytesExt, WriteBytesExt};
use byteview::{ByteView, StrView};
use varint_rs::{VarintReader, VarintWriter};

use crate::keyspace::InternalKeyspaceId;

fn read_u32_len<R: std::io::Read>(reader: &mut R) -> Result<usize, lsm_tree::Error> {
    usize::try_from(reader.read_u32_varint()?).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "DB edit value length does not fit in usize",
        )
        .into()
    })
}

#[derive(Debug)]
pub enum DbEditRecord {
    CreateKeyspace {
        id: InternalKeyspaceId,
        name: StrView,
    },
    ConfigurationChanged {
        id: InternalKeyspaceId,
        k: StrView,
        v: ByteView,
    },

    /// The keyspace was deleted
    ///
    /// Its later version edits (e.g. of a compaction that was still running) are ignored.
    DeleteKeyspace { id: InternalKeyspaceId },

    /// The next keyspace ID to hand out
    ///
    /// IMPORTANT: Keyspace IDs must never be reused, otherwise WAL items of a deleted keyspace
    /// would be replayed into a new keyspace with the same ID. After a manifest rotation,
    /// deleted keyspaces are not in the snapshot anymore, so their IDs are only kept by this.
    SetKeyspaceIdCounter { next: InternalKeyspaceId },

    /// Marks which table and blob file IDs belong to migrated v3 files
    ///
    /// See [`crate::legacy::watermark::LegacyWatermark`].
    SetLegacyWatermark { tables: u64, blob_files: u64 },
}

impl lsm_tree::coding::Encode for DbEditRecord {
    fn encode_into<W: std::io::prelude::Write>(
        &self,
        writer: &mut W,
    ) -> Result<(), lsm_tree::Error> {
        match self {
            Self::CreateKeyspace { id, name } => {
                writer.write_u8(0x0)?; // Enum discriminant
                writer.write_u64_varint(*id)?;

                #[expect(clippy::cast_possible_truncation, reason = "keyspace name is limited")]
                writer.write_u16_varint(name.len() as u16)?;
                writer.write_all(name.as_bytes())?;
            }
            Self::ConfigurationChanged { id, k, v } => {
                writer.write_u8(0x1)?; // Enum discriminant
                writer.write_u64_varint(*id)?;

                #[expect(
                    clippy::cast_possible_truncation,
                    reason = "keyspace config name is limited"
                )]
                writer.write_u16_varint(k.len() as u16)?;
                writer.write_all(k.as_bytes())?;

                #[expect(
                    clippy::cast_possible_truncation,
                    reason = "keyspace config values are really short"
                )]
                writer.write_u32_varint(v.len() as u32)?;
                writer.write_all(v)?;
            }
            Self::DeleteKeyspace { id } => {
                writer.write_u8(0x2)?; // Enum discriminant
                writer.write_u64_varint(*id)?;
            }
            Self::SetKeyspaceIdCounter { next } => {
                writer.write_u8(0x3)?; // Enum discriminant
                writer.write_u64_varint(*next)?;
            }
            Self::SetLegacyWatermark { tables, blob_files } => {
                writer.write_u8(0xF0)?; // Enum discriminant
                writer.write_u64_varint(*tables)?;
                writer.write_u64_varint(*blob_files)?;
            }
        }

        Ok(())
    }
}

impl lsm_tree::coding::Decode for DbEditRecord {
    fn decode_from<R: std::io::prelude::Read>(reader: &mut R) -> Result<Self, lsm_tree::Error>
    where
        Self: Sized,
    {
        match reader.read_u8()? {
            0x0 => {
                let keyspace_id = reader.read_u64_varint()?;

                let name_len = usize::from(reader.read_u16_varint()?);
                let mut name = vec![0; name_len];
                reader.read_exact(&mut name)?;

                let name = std::str::from_utf8(&name)
                    .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;

                Ok(Self::CreateKeyspace {
                    id: keyspace_id,
                    name: name.into(),
                })
            }
            0x1 => {
                let keyspace_id = reader.read_u64_varint()?;

                let key_len = usize::from(reader.read_u16_varint()?);
                let mut key = vec![0; key_len];
                reader.read_exact(&mut key)?;

                let key = std::str::from_utf8(&key)
                    .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;

                let value_len = read_u32_len(reader)?;
                let value = ByteView::from_reader(reader, value_len)?;

                Ok(Self::ConfigurationChanged {
                    id: keyspace_id,
                    k: key.into(),
                    v: value,
                })
            }
            0x2 => {
                let id = reader.read_u64_varint()?;
                Ok(Self::DeleteKeyspace { id })
            }
            0x3 => {
                let next = reader.read_u64_varint()?;
                Ok(Self::SetKeyspaceIdCounter { next })
            }
            0xF0 => {
                let tables = reader.read_u64_varint()?;
                let blob_files = reader.read_u64_varint()?;

                Ok(Self::SetLegacyWatermark { tables, blob_files })
            }
            tag => Err(lsm_tree::Error::InvalidTag(("DbEditRecord", tag))),
        }
    }
}
