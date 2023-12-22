//! An LSM-based embedded key-value storage engine written in Rust.
//!
//! ```
//! use fjall::{Config, Keyspace};
//!
//! # let folder = tempfile::tempdir()?;
//! #
//! // A keyspace is a single database, which houses
//! // multiple partitions ("column families")
//! let keyspace = Config::new(folder).open()?;
//!
//! // Each partition is its own logical keyspace
//! // however modifications may cross partition boundaries
//! // and keep atomic semantics
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
//!
//! For the underlying LSM-tree implementation, see: <https://github.com/marvin-j97/fjall/tree/main/lsm-tree>.
#![doc(html_logo_url = "https://raw.githubusercontent.com/marvin-j97/fjall/main/fjall/logo.png")]
#![doc(html_favicon_url = "https://raw.githubusercontent.com/marvin-j97/fjall/main/fjall/logo.png")]
#![forbid(unsafe_code)]
#![deny(clippy::all, missing_docs, clippy::cargo)]
#![deny(clippy::unwrap_used)]
#![warn(clippy::pedantic, clippy::nursery)]
#![warn(clippy::expect_used)]
#![allow(clippy::missing_const_for_fn)]

mod batch;
mod config;
mod error;
mod file;
mod flush;
mod journal;
mod journal_manager;
mod keyspace;
mod partition;
mod sharded;
mod version;

pub use {
    config::Config,
    error::{Error, Result},
    keyspace::Keyspace,
    partition::PartitionHandle,
};
