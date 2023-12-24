//! An LSM-based embedded key-value storage engine written in Rust.
//!
//! It is not:
//!
//! - a standalone server
//! - a relational database
//! - a wide-column database: it has no notion of columns
//!
//! This crates exports a `Keyspace`, which is a single logical database, which is split
//! into `partitions` (a.k.a. column families). Each partition is logically a single
//! LSM-tree; however, write operations across partitions are atomic as they are persisted
//! in a single database-level journal, which will be recovered after a crash.
//!
//! Keys are limited to 65536 bytes, values are limited to 2^32 bytes. As is normal with any kind of storage
//! engine, larger keys and values have a bigger performance impact.
//!
//! For the underlying LSM-tree implementation, see: <https://crates.io/crates/lsm-tree>.
//!
//! ```
//! use fjall::{Config, Keyspace};
//!
//! # let folder = tempfile::tempdir()?;
//! #
//! let keyspace = Config::new(folder).open()?;
//!
//! // Each partition is its own physical LSM-tree
//! let items = keyspace.open_partition("my_items")?;
//!
//! // Write some data
//! items.insert("a", "hello")?;
//!
//! // And retrieve it
//! let bytes = items.get("a")?;
//!
//! // Or remove it again
//! items.remove("a")?;
//!
//! // Search by prefix
//! for item in &items.prefix("prefix") {
//!   // ...
//! }
//!
//! // Search by range
//! for item in &items.range("a"..="z") {
//!   // ...
//! }
//!
//! // Iterators implement DoubleEndedIterator, so you can search backwards, too!
//! for item in items.prefix("prefix").into_iter().rev() {
//!   // ...
//! }
//!
//! // TODO: (cross-partition) snapshot & batches
//!
//! // Sync the journal to disk to make sure data is definitely durable
//! // When the keyspace is dropped, it will try to persist
//! // Also, by default every second the keyspace will be persisted asynchronously
//! keyspace.persist()?;
//!
//! // Destroy the partition, removing all data in it.
//! // This may be useful when using temporary tables or indexes,
//! // as it is essentially an O(1) operation.
//! items.destroy()?;
//! #
//! # Ok::<_, fjall::Error>(())
//! ```
#![doc(html_logo_url = "https://raw.githubusercontent.com/marvin-j97/fjall/main/fjall/logo.png")]
#![doc(html_favicon_url = "https://raw.githubusercontent.com/marvin-j97/fjall/main/fjall/logo.png")]
#![forbid(unsafe_code)]
#![deny(clippy::all, missing_docs, clippy::cargo)]
#![deny(clippy::unwrap_used)]
#![deny(clippy::indexing_slicing)]
#![warn(clippy::pedantic, clippy::nursery)]
#![warn(clippy::expect_used)]
#![allow(clippy::missing_const_for_fn)]

mod batch;
mod compaction;
mod config;
mod error;
mod file;
mod flush;
mod journal;
mod keyspace;
mod partition;
mod sharded;
mod version;

pub use lsm_tree::compaction as lsm_compaction; // TODO: fix naming conflict
pub use lsm_tree::BlockCache;

pub use {
    config::Config,
    error::{Error, Result},
    keyspace::Keyspace,
    partition::PartitionHandle,
};
