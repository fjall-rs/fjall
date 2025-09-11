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
mod builder;

/// Contains compaction strategies
pub mod compaction;

mod config;

#[cfg(feature = "__internal_whitebox")]
#[doc(hidden)]
pub mod drop;

mod background_worker;
mod db;
mod error;
mod file;
mod flush;
mod gc;
mod iter;
mod journal;
mod keyspace;
mod monitor;
mod path;
mod poison_dart;
mod recovery;
mod snapshot_nonce;
mod snapshot_tracker;
mod stats;
// mod tracked_snapshot;

#[cfg(any(feature = "single_writer_tx", feature = "ssi_tx"))]
mod tx;

mod version;
mod write_buffer_manager;

pub(crate) type HashMap<K, V> = std::collections::HashMap<K, V, xxhash_rust::xxh3::Xxh3Builder>;
pub(crate) type HashSet<K> = std::collections::HashSet<K, xxhash_rust::xxh3::Xxh3Builder>;

pub use {
    batch::Batch as WriteBatch, // TODO: rename Batch -> WriteBatch instead of alias export
    builder::Builder as DatabaseBuilder,
    config::Config,
    db::Database,
    error::{Error, Result},
    gc::GarbageCollection,
    journal::{error::RecoveryError, writer::PersistMode},
    keyspace::{
        options::KvSeparationOptions,
        options::{CreateOptions as KeyspaceCreateOptions, KV_SEPARATION_DEFAULT_THRESHOLD},
        Keyspace,
    },
    // tracked_snapshot::TrackedSnapshot as Snapshot,
    version::Version,
};

#[cfg(any(feature = "single_writer_tx", feature = "ssi_tx"))]
pub use tx::{
    db::TxDatabase, keyspace::TxKeyspace, read_tx::ReadTransaction, write_tx::WriteTransaction,
    Builder as TxDatabaseBuilder,
};

#[doc(hidden)]
pub use lsm_tree::{AbstractTree, Error as LsmError};

pub use lsm_tree::{
    AnyTree, CompressionType, Guard, KvPair, SeqNo, Slice, TreeType, UserKey, UserValue,
};
