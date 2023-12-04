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
//! let tree = Config::new(folder).open()?;
//!
//! assert!(tree.is_empty()?);
//! tree.insert("my_key", "my_value")?;
//! // Note compared to the BTreeMap API, operations return a Result<T>
//! // So you can handle I/O errors if they occur
//!
//! let item = tree.get("my_key")?;
//! assert_eq!(Some("my_value".as_bytes().to_vec()), item);
//!
//! let item = tree.get("another_key")?;
//! assert_eq!(None, item);
//!
//! tree.insert("my_key2", "another item")?;
//! assert_eq!(tree.len()?, 2);
//!
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
#![warn(clippy::expect_used)]
#![deny(clippy::unwrap_used)]

mod batch;
mod block_cache;
pub mod compaction;
mod config;
mod disk_block;
mod disk_block_index;
mod either;
mod entry;
mod error;
mod flush;
mod id;
mod journal;
mod levels;
mod memtable;
mod merge;
mod prefix;
mod range;
mod segment;
mod serde;
mod sharded;
mod snapshot;
mod time;
mod tree;
mod tree_inner;
mod value;

#[doc(hidden)]
pub use value::Value;

pub use {
    crate::serde::{DeserializeError, SerializeError},
    batch::Batch,
    config::Config,
    entry::Entry,
    error::{Error, Result},
    snapshot::Snapshot,
    tree::Tree,
};
