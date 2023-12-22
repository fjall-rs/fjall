//! A K.I.S.S. implementation of log-structured merge trees (LSM-trees/LSMTs).
//!
//! ##### NOTE
//!
//! > This crate only provides the primitives for an LSM-tree.
//! > You probably want to use <https://crates.io/crates/fjall> instead.
//! > For example, it does not ship with a write-ahead log, so writes are not
//! > persisted until manually flushing the memtable.
//!
//! ##### About
//!
//! This crate exports a `Tree` that supports a subset of the `BTreeMap` API.
//!
//! LSM-trees are an alternative to B-trees to persist a sorted list of items (e.g. a database table)
//! on disk and perform fast lookup queries.
//! Instead of updating a disk-based data structure in-place,
//! deltas (inserts and deletes) are added into an in-memory write buffer (`MemTable`).
//! Data is then continuously flushed to disk segments.
//!
//! Amassing many segments on disk will degrade read performance and waste disk space usage, so segments
//! can be periodically merged into larger segments in a process called "Compaction".
//! Different compaction strategies have different advantages and drawbacks, depending on your use case.
//!
//! Because maintaining an efficient structure is deferred to the compaction process, writing to an LSMT
//! is very fast (O(1) complexity).
//!
//! # Example usage
//!
//! ```
//! use lsm_tree::{Tree, Config};
//! #
//! # let folder = tempfile::tempdir()?;
//!
//! // A tree is a single physical keyspace/index/...
//! // and supports a BTreeMap-like API
//! let tree = Config::new(folder).open()?;
//!
//! // Note compared to the BTreeMap API, operations return a Result<T>
//! // So you can handle I/O errors if they occur
//! tree.insert("my_key", "my_value", /* sequence number */ 0);
//!
//! let item = tree.get("my_key")?;
//! assert_eq!(Some("my_value".as_bytes().into()), item);
//!
//! // Search by prefix
//! for item in &tree.prefix("prefix") {
//!   // ...
//! }
//!
//! // Search by range
//! for item in &tree.range("a"..="z") {
//!   // ...
//! }
//!
//! // Iterators implement DoubleEndedIterator, so you can search backwards, too!
//! for item in tree.prefix("prefix").into_iter().rev() {
//!   // ...
//! }
//!
//! // TODO: snapshot
//!
//! // Flush to secondary storage, clearing the memtable
//! // and persisting all in-memory data.
//! tree.flush_active_memtable().expect("should flush").join().unwrap()?;
//! assert_eq!(Some("my_value".as_bytes().into()), item);
//!
//! // When some disk segments have amassed, use compaction
//! // to reduce the amount of disk segents
//!
//! // Choose compaction strategy based on workload
//! use lsm_tree::compaction::Levelled;
//!
//! let strategy = Levelled::default();
//! tree.compact(Box::new(strategy))?;
//!
//! assert_eq!(Some("my_value".as_bytes().into()), item);
//! #
//! # Ok::<(), lsm_tree::Error>(())
//! ```

#![doc(
    html_logo_url = "https://raw.githubusercontent.com/marvin-j97/lsm-tree/main/lsm-tree/logo.png"
)]
#![doc(
    html_favicon_url = "https://raw.githubusercontent.com/marvin-j97/lsm-tree/main/lsm-tree/logo.png"
)]
#![forbid(unsafe_code)]
#![deny(clippy::all, missing_docs, clippy::cargo)]
#![deny(clippy::unwrap_used)]
#![warn(clippy::pedantic, clippy::nursery)]
#![warn(clippy::expect_used)]
#![allow(clippy::missing_const_for_fn)]

mod block_cache;
pub mod compaction;
mod config;
mod descriptor_table;
mod disk_block;
mod either;
mod error;
mod file;
mod flush;
mod id;
mod levels;
mod memtable;
mod merge;

#[doc(hidden)]
pub mod prefix;

#[doc(hidden)]
pub mod range;

mod segment;
mod seqno;
mod serde;
mod sharded;
mod snapshot;
mod stop_signal;
mod time;
mod tree;
mod tree_inner;
mod value;
mod version;

pub use {
    block_cache::BlockCache,
    config::Config,
    error::{Error, Result},
    memtable::MemTable,
    seqno::SequenceNumberCounter,
    serde::{DeserializeError, SerializeError},
    snapshot::Snapshot,
    tree::Tree,
    value::{SeqNo, UserKey, UserValue, Value, ValueType},
};
