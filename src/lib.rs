//! Fjall is an LSM-based embeddable key-value storage engine written in Rust. It features:
//!
//! - Thread-safe BTreeMap-like API
//! - 100% safe & stable Rust
//! - Range & prefix searching with forward and reverse iteration
//! - Cross-partition snapshots (MVCC)
//! - Automatic background maintenance
//! - Single-writer transactions (optional)
//!
//! Each `Keyspace` is a single logical database and is split into `partitions` (a.k.a. column families) - you should probably only use a single keyspace for your application.
//! Each partition is physically a single LSM-tree and its own logical collection; however, write operations across partitions are atomic as they are persisted in a
//! single database-level journal, which will be recovered after a crash.
//!
//! It is not:
//!
//! - a standalone server
//! - a relational database
//! - a wide-column database: it has no notion of columns
//!
//! Keys are limited to 65536 bytes, values are limited to 2^32 bytes. As is normal with any kind of storage engine, larger keys and values have a bigger performance impact.
//!
//! For the underlying LSM-tree implementation, see: <https://crates.io/crates/lsm-tree>.
//!
//! ```
//! use fjall::{Config, PersistMode, Keyspace, PartitionCreateOptions};
//!
//! # let folder = tempfile::tempdir()?;
//! #
//! let keyspace = Config::new(folder).open()?;
//!
//! // Each partition is its own physical LSM-tree
//! let items = keyspace.open_partition("my_items", PartitionCreateOptions::default())?;
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
//! for kv in items.prefix("prefix") {
//!   // ...
//! }
//!
//! // Search by range
//! for kv in items.range("a"..="z") {
//!   // ...
//! }
//!
//! // Iterators implement DoubleEndedIterator, so you can search backwards, too!
//! for kv in items.prefix("prefix").rev() {
//!   // ...
//! }
//!
//! // Sync the journal to disk to make sure data is definitely durable
//! // When the keyspace is dropped, it will try to persist
//! // Also, by default every second the keyspace will be persisted asynchronously
//! keyspace.persist(PersistMode::SyncAll)?;
//!
//! // Destroy the partition, removing all data in it.
//! // This may be useful when using temporary tables or indexes,
//! // as it is essentially an O(1) operation.
//! keyspace.delete_partition(items)?;
//! #
//! # Ok::<_, fjall::Error>(())
//! ```

#![doc(html_logo_url = "https://raw.githubusercontent.com/fjall-rs/fjall/main/logo.png")]
#![doc(html_favicon_url = "https://raw.githubusercontent.com/fjall-rs/fjall/main/logo.png")]
#![forbid(unsafe_code)]
#![deny(clippy::all, missing_docs, clippy::cargo)]
#![deny(clippy::unwrap_used)]
#![deny(clippy::indexing_slicing)]
#![warn(clippy::pedantic, clippy::nursery)]
#![warn(clippy::expect_used)]
#![allow(clippy::missing_const_for_fn)]

mod batch;

/// Contains compaction strategies
pub mod compaction;

mod config;
mod error;
mod file;
mod flush;
mod journal;
mod keyspace;
mod monitor;
mod partition;
mod recovery;
mod sharded;

#[cfg(feature = "single_writer_tx")]
mod tx;

mod version;
mod write_buffer_manager;

pub use {
    batch::Batch,
    config::Config,
    error::{Error, Result},
    journal::{shard::RecoveryError, writer::PersistMode},
    keyspace::Keyspace,
    partition::{config::CreateOptions as PartitionCreateOptions, PartitionHandle},
    version::Version,
};

#[cfg(feature = "single_writer_tx")]
pub use tx::{
    keyspace::{TransactionalKeyspace, TxKeyspace},
    partition::TransactionalPartitionHandle,
    read_tx::ReadTransaction,
    write_tx::WriteTransaction,
};

/// Alias for [`PartitionHandle`]
pub type Partition = PartitionHandle;

/// Alias for [`TransactionalPartitionHandle`]
#[cfg(feature = "single_writer_tx")]
pub type TxPartition = TransactionalPartitionHandle;

/// Alias for [`TransactionalPartitionHandle`]
#[cfg(feature = "single_writer_tx")]
pub type TxPartitionHandle = TransactionalPartitionHandle;

/// Alias for [`TransactionalPartitionHandle`]
#[cfg(feature = "single_writer_tx")]
pub type TransactionalPartition = TransactionalPartitionHandle;

/// A snapshot moment
///
/// See [`Keyspace::instant`].
pub type Instant = lsm_tree::SeqNo;

/// Re-export of [`lsm_tree::Error`]
pub type LsmError = lsm_tree::Error;

pub use lsm_tree::{AnyTree, BlockCache, KvPair, Snapshot, TreeType, UserKey, UserValue};
