//! A K.I.S.S. implementation of log-structured merge trees (LSM-trees/LSMTs).

// #![doc(html_logo_url = "TODO:")]
// #![doc(html_favicon_url = "TODO:")]
#![deny(clippy::all, missing_docs, clippy::cargo)]
#![warn(clippy::pedantic, clippy::nursery)]
#![forbid(unsafe_code)]

mod block_cache;
mod commit_log;
mod config;
mod disk_block;
mod disk_block_index;
mod error;
mod memtable;
mod segment;
mod serde;
mod tree;
mod tree_inner;
mod value;

pub use {
    crate::serde::{DeserializeError, SerializeError},
    config::Config,
    error::{Error, Result},
    tree::Tree,
    value::Value,
};
