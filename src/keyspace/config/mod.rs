// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

mod block_size;
mod compression;
mod filter;
mod hash_ratio;
mod pinning;
mod restart_interval;

pub trait EncodeConfig {
    fn encode(&self) -> crate::Slice;
}

// TODO: result
pub trait DecodeConfig {
    fn decode(bytes: &[u8]) -> crate::Result<Self>
    where
        Self: Sized;
}
