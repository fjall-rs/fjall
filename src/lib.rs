//! A K.I.S.S. implementation of log-structured merge trees (LSM-trees/LSMTs).

// #![doc(html_logo_url = "TODO:")]
// #![doc(html_favicon_url = "TODO:")]
#![deny(clippy::all, missing_docs, clippy::cargo)]
#![warn(clippy::pedantic, clippy::nursery)]
#![forbid(unsafe_code)]

mod commit_log;
mod error;
mod memtable;
mod segment;
mod serde;
mod value;

pub use {
    crate::serde::{DeserializeError, SerializeError},
    error::{Error, Result},
    value::Value,
};
