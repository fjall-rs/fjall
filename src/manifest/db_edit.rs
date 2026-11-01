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
            tag => Err(lsm_tree::Error::InvalidTag(("DbEditRecord", tag))),
        }
    }
}
