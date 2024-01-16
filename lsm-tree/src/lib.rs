//! A K.I.S.S. implementation of log-structured merge trees (LSM-trees/LSMTs).
//!
//! ##### NOTE
//!
//! > This crate only provides a primitive LSM-tree, not a full storage engine.
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
//! Data is then flushed to disk segments, as the write buffer reaches some threshold.
//!
//! Amassing many segments on disk will degrade read performance and waste disk space usage, so segments
//! can be periodically merged into larger segments in a process called `Compaction`.
//! Different compaction strategies have different advantages and drawbacks, and should be chosen based
//! on the workload characteristics.
//!
//! Because maintaining an efficient structure is deferred to the compaction process, writing to an LSMT
//! is very fast (O(1) complexity).
//!
//! Keys are limited to 65536 bytes, values are limited to 2^32 bytes. As is normal with any kind of storage
//! engine, larger keys and values have a bigger performance impact.
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
//! // Flush to secondary storage, clearing the memtable
//! // and persisting all in-memory data.
//! // Note, this flushes synchronously, which may not be desired
//! tree.flush_active_memtable()?;
//! assert_eq!(Some("my_value".as_bytes().into()), item);
//!
//! // When some disk segments have amassed, use compaction
//! // to reduce the amount of disk segents
//!
//! // Choose compaction strategy based on workload
//! use lsm_tree::compaction::Levelled;
//! # use std::sync::Arc;
//!
//! let strategy = Levelled::default();
//! tree.compact(Arc::new(strategy))?;
//!
//! assert_eq!(Some("my_value".as_bytes().into()), item);
//! #
//! # Ok::<(), lsm_tree::Error>(())
//! ```

#![doc(html_logo_url = "https://raw.githubusercontent.com/fjall-rs/fjall/main/lsm-tree/logo.png")]
#![doc(
    html_favicon_url = "https://raw.githubusercontent.com/fjall-rs/fjall/main/lsm-tree/logo.png"
)]
#![forbid(unsafe_code)]
#![deny(clippy::all, missing_docs, clippy::cargo)]
#![deny(clippy::unwrap_used)]
#![deny(clippy::indexing_slicing)]
#![warn(clippy::pedantic, clippy::nursery)]
#![warn(clippy::expect_used)]
#![allow(clippy::missing_const_for_fn)]

#[cfg(not(target_pointer_width = "64"))]
compile_error!("compilation is only allowed for 64-bit targets");

mod block_cache;

#[doc(hidden)]
#[cfg(feature = "bloom")]
pub mod bloom;

pub mod compaction;
mod config;

#[doc(hidden)]
pub mod descriptor_table;

mod disk_block;
mod either;
mod error;

#[doc(hidden)]
pub mod file;

#[doc(hidden)]
pub mod flush;

#[doc(hidden)]
pub mod id;

mod levels;

mod lru_list;

mod memtable;
mod merge;

#[doc(hidden)]
pub mod prefix;

#[doc(hidden)]
pub mod range;

#[doc(hidden)]
pub mod segment;

mod seqno;

#[doc(hidden)]
pub mod serde;

mod snapshot;

#[doc(hidden)]
pub mod stop_signal;

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
    segment::Segment,
    seqno::SequenceNumberCounter,
    serde::{DeserializeError, SerializeError},
    snapshot::Snapshot,
    tree::Tree,
    value::{SeqNo, UserKey, UserValue, Value, ValueType},
};
