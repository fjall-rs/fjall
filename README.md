<p align="center">
  <img src="/logo.png" height="128">
</p>
<p align="center>
  (temporary logo)
</p>

[![CI](https://github.com/fjall-rs/fjall/actions/workflows/test.yml/badge.svg)](https://github.com/fjall-rs/fjall/actions/workflows/test.yml)
[![docs.rs](https://img.shields.io/docsrs/fjall?color=green)](https://docs.rs/fjall)
[![Crates.io](https://img.shields.io/crates/v/fjall?color=blue)](https://crates.io/crates/fjall)
![MSRV](https://img.shields.io/badge/MSRV-1.74.0-blue)

Fjall is an LSM-based embeddable key-value storage engine written in Rust. It features:

- Thread-safe BTreeMap-like API
- 100% safe & stable Rust
- Range & prefix searching with forward and reverse iteration
- Cross-partition snapshots (MVCC)
- Automatic background maintenance

Each `Keyspace` is a single logical database and is split into `partitions` (a.k.a. column families) - you should probably only use a single keyspace for your application. Each partition is physically a single LSM-tree and its own logical collection; however, write operations across partitions are atomic as they are persisted in a single database-level journal, which will be recovered on restart.

It is not:

- a standalone server
- a relational database
- a wide-column database: it has no notion of columns

Keys are limited to 65536 bytes, values are limited to 2^32 bytes. As is normal with any kind of storage engine, larger keys and values have a bigger performance impact.

For the underlying LSM-tree implementation, see: <https://crates.io/crates/lsm-tree>.

## Basic usage

```bash
cargo add fjall
```

```rust
use fjall::{Config, FlushMode, Keyspace, PartitionCreateOptions};

let keyspace = Config::new(folder).open()?;

// Each partition is its own physical LSM-tree
let items = keyspace.open_partition("my_items", PartitionCreateOptions::default())?;

// Write some data
items.insert("a", "hello")?;

// And retrieve it
let bytes = items.get("a")?;

// Or remove it again
items.remove("a")?;

// Search by prefix
for item in &items.prefix("prefix") {
  // ...
}

// Search by range
for item in &items.range("a"..="z") {
  // ...
}

// Iterators implement DoubleEndedIterator, so you can search backwards, too!
for item in items.prefix("prefix").into_iter().rev() {
  // ...
}

// Atomic write batches (multiple partitions can be used in a single batch)
let mut batch = keyspace.batch();
batch.insert(&items, "1", "abc");
batch.insert(&items, "3", "abc");
batch.insert(&items, "5", "abc");
batch.commit()?;

// Sync the journal to disk to make sure data is definitely durable
// When the keyspace is dropped, it will try to persist
// Also, by default every second the keyspace will be persisted asynchronously
keyspace.persist(FlushMode::SyncAll)?;

// Destroy the partition, removing all data in it.
// This may be useful when using temporary tables or indexes,
// as it is essentially an O(1) operation.
keyspace.delete_partition(items)?;
```

## Details

- Partitions (a.k.a. column families) with cross-partition atomic semantics (atomic write batches)
- Sharded journal for concurrent writes
- Cross-partition snapshots (MVCC)
- anything else implemented in [`lsm-tree`](https://github.com/fjall-rs/lsm-tree)

## Durability

To support different kinds of workloads, Fjall is agnostic about the type of durability
your application needs. After writing data (be it, `insert`, `remove` or committing a write batch), you can choose to call `Keyspace::persist` which takes a [`FlushMode`](https://docs.rs/fjall/latest/fjall/enum.FlushMode.html) parameter.

## Features

#### bloom

Uses bloom filters to reduce disk I/O for non-existing keys. Improves point read performance, but increases memory usage.

*Disabled by default.*

## Stable disk format

The disk format is stable as of 1.0.0. Future breaking changes will result in a major version bump and a migration path.

## Examples

[See here](https://github.com/fjall-rs/fjall/tree/main/examples) for practical examples.

And checkout [`Smoltable`](https://github.com/marvin-j97/smoltable), a standalone Bigtable-inspired mini wide-column database using `fjall` as its storage engine.

## Contributing

How can you help?

- [Ask a question](https://github.com/fjall-rs/fjall/discussions/new?category=q-a)
- [Post benchmarks and things you created](https://github.com/fjall-rs/fjall/discussions/new?category=show-and-tell)
- [Open an issue](https://github.com/fjall-rs/fjall/issues/new) (bug report, weirdness)
- [Open a PR](https://github.com/fjall-rs/fjall/compare)

## License

All source code is licensed under MIT OR Apache-2.0.

All contributions are to be licensed as MIT OR Apache-2.0.
