// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use byteorder::{ReadBytesExt, WriteBytesExt};
use byteview::StrView;
use varint_rs::{VarintReader, VarintWriter};

use crate::keyspace::InternalKeyspaceId;

#[derive(Debug)]
pub enum DbEditRecord {
    CreateKeyspace {
        id: InternalKeyspaceId,
        name: StrView,
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
                writer.write_all(name.as_bytes())?;
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

                let mut name = Vec::new();
                reader.read_to_end(&mut name)?;

                let name = std::str::from_utf8(&name)
                    .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;

                Ok(Self::CreateKeyspace {
                    id: keyspace_id,
                    name: name.into(),
                })
            }
            tag => Err(lsm_tree::Error::InvalidTag(("CustomRecord", tag))),
        }
    }
}
