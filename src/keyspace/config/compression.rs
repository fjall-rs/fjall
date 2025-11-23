// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::keyspace::config::{DecodeConfig, EncodeConfig};
use byteorder::{ReadBytesExt, WriteBytesExt};
use lsm_tree::{
    coding::{Decode, Encode},
    CompressionType,
};

impl EncodeConfig for crate::config::CompressionPolicy {
    fn encode(&self) -> crate::Slice {
        let mut v = vec![];

        // NOTE: Policies are limited to 255 entries
        #[expect(clippy::cast_possible_truncation)]
        #[expect(clippy::expect_used)]
        v.write_u8(self.len() as u8)
            .expect("cannot fail writing into a vec");

        for item in self.iter() {
            #[expect(clippy::expect_used)]
            item.encode_into(&mut v)
                .expect("cannot fail writing into a vec");
        }

        v.into()
    }
}

impl DecodeConfig for crate::config::CompressionPolicy {
    fn decode(mut bytes: &[u8]) -> crate::Result<Self> {
        let len = bytes.read_u8()?;

        let mut v = vec![];

        for _ in 0..len {
            v.push(CompressionType::decode_from(&mut bytes)?);
        }

        Ok(Self::new(v))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_log::test;

    #[test]
    #[cfg(feature = "lz4")]
    fn roundtrip_compression_policy() -> crate::Result<()> {
        let policy =
            crate::config::CompressionPolicy::new([CompressionType::None, CompressionType::Lz4]);
        let encoded = policy.encode();
        let decoded = crate::config::CompressionPolicy::decode(&encoded)?;
        assert_eq!(policy, decoded);
        Ok(())
    }
}
