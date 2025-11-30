// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

//! Fjall is a log-structured embeddable key-value storage engine written in Rust. It features:
//!
//! - A thread-safe BTreeMap-like API
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
//! - A standalone database server
//! - A relational or wide-column database: it has no notion of columns or query language
//!
//! Keys are limited to 65536 bytes, values are limited to 2^32 bytes. As is normal with any kind of storage engine, larger keys and values have a bigger performance impact.
//!
//! For the underlying LSM-tree implementation, see: <https://crates.io/crates/lsm-tree>.
//!
//! ## Basic usage
//!
//! ```
//! use fjall::{PersistMode, Database, KeyspaceCreateOptions};
//! #
//! # let folder = tempfile::tempdir().unwrap();
//!
//! // A database may contain multiple keyspaces
//! // You should probably only use a single database for your application
//! let db = Database::builder(&folder).open()?;
//! // TxDatabase::builder for transactional semantics
//!
//! // Each keyspace is its own physical LSM-tree
//! let items = db.keyspace("my_items", KeyspaceCreateOptions::default)?;
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
//! for kv in items.prefix("user1") {
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
#![deny(unsafe_code)]
#![deny(clippy::all, missing_docs, clippy::cargo)]
#![deny(clippy::unwrap_used)]
#![deny(clippy::indexing_slicing)]
#![warn(clippy::pedantic, clippy::nursery)]
#![warn(clippy::expect_used)]
#![allow(clippy::missing_const_for_fn, clippy::significant_drop_tightening)]
#![warn(clippy::multiple_crate_versions)]
#![cfg_attr(docsrs, feature(doc_cfg))]

macro_rules! fail_iter {
    ($e:expr) => {
        match $e {
            Ok(v) => v,
            Err(e) => return Some(Err(e.into())),
        }
    };
}

mod backpressure;
mod batch;
mod builder;

/// Contains compaction strategies
pub mod compaction;

mod db_config;

#[cfg(feature = "__internal_whitebox")]
#[doc(hidden)]
pub mod drop;

mod db;

#[cfg(test)]
mod db_test;

mod error;
mod file;
mod flush;
mod guard;
mod iter;
mod journal;
mod keyspace;
mod locked_file;
mod meta_keyspace;
mod path;
mod poison_dart;
mod readable;
mod recovery;
mod snapshot;
mod snapshot_nonce;
mod snapshot_tracker;
mod stats;
mod supervisor;
mod tx;
mod version;
mod worker_pool;
mod write_buffer_manager;

pub(crate) type HashMap<K, V> = std::collections::HashMap<K, V, xxhash_rust::xxh3::Xxh3Builder>;

/// Configuration policies
pub mod config {
    pub use lsm_tree::config::{
        BlockSizePolicy, BloomConstructionPolicy, CompressionPolicy, FilterPolicy,
        FilterPolicyEntry, HashRatioPolicy, PartioningPolicy, PinningPolicy, RestartIntervalPolicy,
    };
}

pub use {
    batch::WriteBatch as OwnedWriteBatch,
    builder::Builder as DatabaseBuilder,
    db::Database,
    db_config::Config,
    error::{Error, Result},
    guard::Guard,
    iter::Iter,
    journal::{error::RecoveryError as JournalRecoveryError, writer::PersistMode},
    keyspace::{options::CreateOptions as KeyspaceCreateOptions, Keyspace},
    readable::Readable,
    snapshot::Snapshot,
    version::FormatVersion,
};

pub use tx::single_writer::{
    SingleWriterTxKeyspace, TxDatabase as SingleWriterTxDatabase,
    WriteTransaction as SingleWriterWriteTx,
};

pub use tx::optimistic::{
    Conflict, OptimisticTxDatabase, OptimisticTxKeyspace, WriteTransaction as OptimisticWriteTx,
};

#[doc(hidden)]
pub use lsm_tree::{AbstractTree, AnyTree, Error as LsmError, TreeType};

pub use lsm_tree::{
    CompressionType, KvPair, KvSeparationOptions, SeqNo, Slice, UserKey, UserValue,
};

/// Utility functions
pub mod util {
    pub use lsm_tree::util::{prefix_to_range, prefixed_range};
}
