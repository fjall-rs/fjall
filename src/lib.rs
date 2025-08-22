// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

//! Fjall is a log-structured embeddable key-value storage engine written in Rust. It features:
//!
//! - Thread-safe BTreeMap-like API
//! - 100% safe & stable Rust
//! - LSM-tree-based storage similar to `RocksDB`
//! - Range & prefix searching with forward and reverse iteration
//! - Keyspaces (a.k.a. column families) with cross-keyspace atomic semantics
//! - Built-in compression (default = `LZ4`)
//! - Serializable transactions (optional)
//! - Key-value separation for large blob use cases (optional)
//! - Automatic background maintenance
//!
//! It is not:
//!
//! - a standalone server
//! - a relational or wide-column database: it has no notion of columns
//!
//! Keys are limited to 65536 bytes, values are limited to 2^32 bytes. As is normal with any kind of storage engine, larger keys and values have a bigger performance impact.
//!
//! For the underlying LSM-tree implementation, see: <https://crates.io/crates/lsm-tree>.
//!
//! ```
//! use fjall::{Config, PersistMode, Database, KeyspaceCreateOptions};
//!
//! // A database may contain multiple keyspaces
//! // You should probably only use a single database for your application
//! let db = Database::builder(folder).open()?;
//! // TxDatabase::builder for transactional semantics
//!
//! // Each keyspace is its own physical LSM-tree
//! let items = db.keyspace("my_items", KeyspaceCreateOptions::default())?;
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
//! // When the database is dropped, it will try to persist with `PersistMode::SyncAll` as well
//! db.persist(PersistMode::SyncAll)?;
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
#![warn(clippy::multiple_crate_versions)]
#![cfg_attr(docsrs, feature(doc_cfg))]

mod batch;

/// Contains compaction strategies
pub mod compaction;

mod config;

#[cfg(feature = "__internal_whitebox")]
#[doc(hidden)]
pub mod drop;

mod background_worker;
mod error;
mod file;
mod flush;
mod gc;
mod iter;
mod journal;
mod keyspace;
mod monitor;
mod partition;
mod path;
mod poison_dart;
mod recovery;
mod snapshot_nonce;
mod snapshot_tracker;
mod stats;
mod tracked_snapshot;

#[cfg(any(feature = "single_writer_tx", feature = "ssi_tx"))]
mod tx;

mod version;
mod write_buffer_manager;

pub(crate) type HashMap<K, V> = std::collections::HashMap<K, V, xxhash_rust::xxh3::Xxh3Builder>;
pub(crate) type HashSet<K> = std::collections::HashSet<K, xxhash_rust::xxh3::Xxh3Builder>;

pub use {
    batch::Batch,
    config::Config,
    error::{Error, Result},
    gc::GarbageCollection,
    journal::{error::RecoveryError, writer::PersistMode},
    keyspace::Keyspace,
    partition::{
        options::CreateOptions as PartitionCreateOptions, options::KvSeparationOptions,
        PartitionHandle,
    },
    tracked_snapshot::TrackedSnapshot as Snapshot,
    version::Version,
};

#[cfg(any(feature = "single_writer_tx", feature = "ssi_tx"))]
pub use tx::{
    keyspace::{TransactionalKeyspace, TxKeyspace},
    partition::TransactionalPartitionHandle,
    read_tx::ReadTransaction,
    write_tx::WriteTransaction,
};

/// Alias for [`Batch`]
pub type WriteBatch = Batch;

/// Alias for [`PartitionHandle`]
pub type Partition = PartitionHandle;

/// Alias for [`TransactionalPartitionHandle`]
#[cfg(any(feature = "single_writer_tx", feature = "ssi_tx"))]
pub type TxPartition = TransactionalPartitionHandle;

/// Alias for [`TransactionalPartitionHandle`]
#[cfg(any(feature = "single_writer_tx", feature = "ssi_tx"))]
pub type TxPartitionHandle = TransactionalPartitionHandle;

/// Alias for [`TransactionalPartitionHandle`]
#[cfg(any(feature = "single_writer_tx", feature = "ssi_tx"))]
pub type TransactionalPartition = TransactionalPartitionHandle;

/// A snapshot moment
///
/// See [`Keyspace::instant`].
pub type Instant = lsm_tree::SeqNo;

/// Re-export of [`lsm_tree::Error`]
pub type LsmError = lsm_tree::Error;

#[doc(hidden)]
pub use lsm_tree::AbstractTree;

pub use lsm_tree::{AnyTree, CompressionType, KvPair, Slice, TreeType, UserKey, UserValue};

// TODO: remove in V3

/// Block cache that caches frequently read disk blocks
#[deprecated = "Use Config::cache_size instead"]
pub struct BlockCache(u64);

#[allow(deprecated)]
impl BlockCache {
    /// Creates a new cache with given capacity in bytes.
    #[must_use]
    pub fn with_capacity_bytes(bytes: u64) -> Self {
        Self(bytes)
    }

    /// Returns the cache capacity in bytes.
    #[must_use]
    pub fn capacity(&self) -> u64 {
        self.0
    }
}

/// Blob cache that caches frequently read blobs
#[deprecated = "Use Config::cache_size instead"]
#[allow(deprecated)]
pub struct BlobCache(BlockCache);

#[allow(deprecated)]
impl BlobCache {
    /// Creates a new cache with given capacity in bytes.
    #[must_use]
    pub fn with_capacity_bytes(bytes: u64) -> Self {
        #[allow(deprecated)]
        Self(BlockCache::with_capacity_bytes(bytes))
    }
}

#[allow(deprecated)]
impl std::ops::Deref for BlobCache {
    type Target = BlockCache;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
