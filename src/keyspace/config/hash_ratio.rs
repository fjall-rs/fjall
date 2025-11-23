// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::keyspace::config::{DecodeConfig, EncodeConfig};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

impl EncodeConfig for crate::config::HashRatioPolicy {
    fn encode(&self) -> crate::Slice {
        let mut v = vec![];

        // NOTE: Policies are limited to 255 entries
        #[expect(clippy::cast_possible_truncation)]
        #[expect(clippy::expect_used)]
        v.write_u8(self.len() as u8)
            .expect("cannot fail writing into a vec");

        for item in self.iter() {
            #[expect(clippy::expect_used)]
            v.write_f32::<LittleEndian>(*item)
                .expect("cannot fail writing into a vec");
        }

        v.into()
    }
}

impl DecodeConfig for crate::config::HashRatioPolicy {
    fn decode(mut bytes: &[u8]) -> crate::Result<Self> {
        let len = bytes.read_u8()?;

        let mut v = vec![];

        for _ in 0..len {
            v.push(bytes.read_f32::<LittleEndian>()?);
        }

        Ok(Self::new(v))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_log::test;

    #[test]
    fn roundtrip_hash_ratio_policy() -> crate::Result<()> {
        let policy = crate::config::HashRatioPolicy::new([0.1, 0.2, 0.3]);
        let encoded = policy.encode();
        let decoded = crate::config::HashRatioPolicy::decode(&encoded)?;
        assert_eq!(policy, decoded);
        Ok(())
    }
}
