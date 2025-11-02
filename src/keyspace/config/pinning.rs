// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::keyspace::config::{DecodeConfig, EncodeConfig};
use byteorder::{ReadBytesExt, WriteBytesExt};

impl EncodeConfig for crate::config::PinningPolicy {
    fn encode(&self) -> crate::Slice {
        let mut v = vec![];

        // NOTE: Policies are limited to 255 entries
        #[allow(clippy::cast_possible_truncation)]
        #[allow(clippy::expect_used)]
        v.write_u8(self.len() as u8).expect("cannot fail");

        for item in self.iter() {
            #[allow(clippy::expect_used)]
            v.write_u8(u8::from(*item)).expect("cannot fail");
        }

        v.into()
    }
}

impl DecodeConfig for crate::config::PinningPolicy {
    fn decode(mut bytes: &[u8]) -> Self {
        let len = bytes.read_u8().expect("cannot fail");

        let mut v = vec![];

        for _ in 0..len {
            let b = bytes.read_u8().expect("cannot fail");
            v.push(b == 1);
        }

        Self::new(v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_log::test;

    #[test]
    fn roundtrip_pinning_policy() {
        let policy = crate::config::PinningPolicy::new([true, true, false]);
        let encoded = policy.encode();
        let decoded = crate::config::PinningPolicy::decode(&encoded);
        assert_eq!(policy, decoded);
    }
}
