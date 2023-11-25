//! A K.I.S.S. implementation of log-structured merge trees (LSM-trees/LSMTs).
//!
//! This crate exports a persistent `Tree` that supports a subset of the `BTreeMap` API.
//!
//! LSM-trees are an alternative to B-trees to persist a sorted list of items (e.g. a database table) on disk and perform fast lookup queries.
//! Instead of updating a disk-based data structure in-place, deltas (inserts and deletes) are added into an in-memory data structure (`MemTable`).
//! To provide durability, the `MemTable` is backed by a disk-based journal.
//! When the `MemTable` exceeds a size threshold, it is flushed to a sorted file on disk ("Segment" a.k.a. "`SSTable`").
//! Items can then be retrieved by checking and merging results from the `MemTable` and Segments.
//!
//! Amassing many segments on disk will degrade read performance and waste disk space usage, so segments are periodically merged into larger segments in a process called "Compaction".
//! Different compaction strategies have different advantages and drawbacks, depending on your use case.
//!
//! Because maintaining an efficient structure is deferred to the compaction process, writing to a LSMT is very fast (O(1) complexity).
//!
//! # Example usage
//!
//! ```
//! use lsm_tree::{Tree, Config};
//!
//! # let folder = tempfile::tempdir()?;
//! let mut tree = Config::new(folder).open()?;
//!
//! assert!(tree.is_empty()?);
//! tree.insert("my_key", "this is the actual value of the object")?;
//! let item = tree.get("my_key")?;
//! assert!(item.is_some());
//! let item = item.unwrap();
//! assert_eq!(item.key, "my_key".as_bytes());
//! assert_eq!(item.value, "this is the actual value of the object".as_bytes());
//!
//! let item = tree.get("another_key")?;
//! assert!(item.is_none());
//!
//! tree.insert("my_key2", "another item")?;
//! assert_eq!(tree.len()?, 2);
//! tree.remove("my_key")?;
//! assert_eq!(tree.len()?, 1);
//!
//! # Ok::<(), lsm_tree::Error>(())
//! ```

// #![doc(html_logo_url = "TODO:")]
// #![doc(html_favicon_url = "TODO:")]
#![deny(clippy::all, missing_docs, clippy::cargo)]
#![warn(clippy::pedantic, clippy::nursery)]
#![forbid(unsafe_code)]
#![allow(clippy::missing_const_for_fn)]

mod batch;
mod block_cache;
mod commit_log;
pub mod compaction;
mod config;
mod disk_block;
mod disk_block_index;
mod either;
mod error;
mod flush;
mod id;
mod level;
mod memtable;
mod merge;
mod prefix;
mod range;
mod segment;
mod serde;
mod time;
mod tree;
mod tree_inner;
mod value;

pub use {
    crate::serde::{DeserializeError, SerializeError},
    batch::Batch,
    config::Config,
    error::{Error, Result},
    tree::Tree,
    value::Value,
};
