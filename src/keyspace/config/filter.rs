// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::keyspace::config::{DecodeConfig, EncodeConfig};
use byteorder::{ReadBytesExt, WriteBytesExt};

impl EncodeConfig for crate::config::FilterPolicy {
    fn encode(&self) -> crate::Slice {
        let mut v = vec![];

        // NOTE: Policies are limited to 255 entries
        #[allow(clippy::cast_possible_truncation)]
        #[allow(clippy::expect_used)]
        v.write_u8(self.len() as u8).expect("cannot fail");

        for item in self.iter() {
            match item {
                crate::config::FilterPolicyEntry::None => {
                    v.write_u8(0).expect("cannot fail");
                }
                crate::config::FilterPolicyEntry::Bloom(bloom) => {
                    v.write_u8(1).expect("cannot fail");

                    match bloom {
                        crate::config::BloomConstructionPolicy::BitsPerKey(bits) => {
                            v.write_u8(0).expect("cannot fail");
                            v.write_f32::<byteorder::LittleEndian>(*bits)
                                .expect("cannot fail");
                        }
                        crate::config::BloomConstructionPolicy::FpRate(fpr) => {
                            v.write_u8(1).expect("cannot fail");
                            v.write_f32::<byteorder::LittleEndian>(*fpr)
                                .expect("cannot fail");
                        }
                    };
                }
            };
        }

        v.into()
    }
}

impl DecodeConfig for crate::config::FilterPolicy {
    fn decode(mut bytes: &[u8]) -> Self {
        let len = bytes.read_u8().expect("cannot fail");

        let mut v = vec![];

        for _ in 0..len {
            let tag = bytes.read_u8().expect("cannot fail");

            match tag {
                0 => {
                    v.push(crate::config::FilterPolicyEntry::None);
                }
                1 => {
                    let policy_type = bytes.read_u8().expect("cannot fail");

                    let policy = match policy_type {
                        0 => {
                            let bits = bytes
                                .read_f32::<byteorder::LittleEndian>()
                                .expect("cannot fail");

                            crate::config::FilterPolicyEntry::Bloom(
                                crate::config::BloomConstructionPolicy::BitsPerKey(bits),
                            )
                        }
                        1 => {
                            let value = bytes
                                .read_f32::<byteorder::LittleEndian>()
                                .expect("cannot fail");

                            crate::config::FilterPolicyEntry::Bloom(
                                crate::config::BloomConstructionPolicy::FpRate(value),
                            )
                        }
                        _ => {
                            panic!("unknown bloom filter policy type: {policy_type}");
                        }
                    };

                    v.push(policy);
                }
                _ => {
                    panic!("unknown filter policy tag: {tag}");
                }
            }
        }

        Self::new(&v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_log::test;

    #[test]
    fn roundtrip_filter_policy() {
        let policy = crate::config::FilterPolicy::new(&[
            crate::config::FilterPolicyEntry::Bloom(
                crate::config::BloomConstructionPolicy::BitsPerKey(10.0),
            ),
            crate::config::FilterPolicyEntry::Bloom(
                crate::config::BloomConstructionPolicy::FpRate(0.01),
            ),
            crate::config::FilterPolicyEntry::None,
        ]);

        let encoded = policy.encode();
        let decoded = crate::config::FilterPolicy::decode(&encoded);

        assert_eq!(policy, decoded);
    }
}
