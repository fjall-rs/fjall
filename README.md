<p align="center">
  <img src="/kawaii.png" height="200">
</p>

<p align="center">
  <a href="https://github.com/fjall-rs/fjall/actions/workflows/test.yml">
    <img src="https://github.com/fjall-rs/fjall/actions/workflows/test.yml/badge.svg" alt="CI" />
  </a>
  <a href="https://docs.rs/fjall">
    <img src="https://img.shields.io/docsrs/fjall?color=green" alt="docs.rs" />
  </a>
  <a href="https://crates.io/crates/fjall">
    <img src="https://img.shields.io/crates/v/fjall?color=blue" alt="Crates.io" />
  </a>
  <img src="https://img.shields.io/badge/MSRV-1.74.0-blue" alt="MSRV" />
  <a href="https://discord.com/invite/HvYGp4NFFk">
    <img src="https://img.shields.io/discord/1240426554111164486" alt="Discord" />
  </a>
  <a href="https://deps.rs/repo/github/fjall-rs/fjall">
    <img src="https://deps.rs/repo/github/fjall-rs/fjall/status.svg" alt="dependency status" />
  </a>
</p>

*Fjall* is an LSM-based embeddable key-value storage engine written in Rust. It features:

- Thread-safe BTreeMap-like API
- 100% safe & stable Rust
- Range & prefix searching with forward and reverse iteration
- Automatic background maintenance
- Partitions (a.k.a. column families) with cross-partition atomic semantics
- Built-in compression (default = LZ4)
- Serializable transactions (optional)
- Key-value separation for large blob use cases (optional)

It is not:

- a standalone server
- a relational database
- a wide-column database: it has no built-in notion of columns

Keys are limited to 65536 bytes, values are limited to 2^32 bytes. As is normal with any kind of storage engine, larger keys and values have a bigger performance impact.

Like any typical key-value store, keys are stored in lexicographic order. If you are storing integer keys (e.g. timeseries data), you should use the big endian form to adhere to locality.

## Basic usage

```bash
cargo add fjall
```

```rust
use fjall::{Config, PersistMode, Keyspace, PartitionCreateOptions};

// A keyspace is a database, which may contain multiple collections ("partitions")
// You should probably only use a single keyspace for your application
//
let keyspace = Config::new(folder).open()?; // or open_transactional for transactional semantics

// Each partition is its own physical LSM-tree
let items = keyspace.open_partition("my_items", PartitionCreateOptions::default())?;

// Write some data
items.insert("a", "hello")?;

// And retrieve it
let bytes = items.get("a")?;

// Or remove it again
items.remove("a")?;

// Search by prefix
for kv in items.prefix("prefix") {
  // ...
}

// Search by range
for kv in items.range("a"..="z") {
  // ...
}

// Iterators implement DoubleEndedIterator, so you can search backwards, too!
for kv in items.prefix("prefix").rev() {
  // ...
}

// Sync the journal to disk to make sure data is definitely durable
// When the keyspace is dropped, it will try to persist with `PersistMode::SyncAll` as well
keyspace.persist(PersistMode::SyncAll)?;
```

## Durability

To support different kinds of workloads, Fjall is agnostic about the type of durability
your application needs.
After writing data (`insert`, `remove` or committing a write batch/transaction), you can choose to call [`Keyspace::persist`](https://docs.rs/fjall/latest/fjall/struct.Keyspace.html#method.persist) which takes a [`PersistMode`](https://docs.rs/fjall/latest/fjall/enum.PersistMode.html) parameter.
By default, any operation will flush to OS buffers, but **not** to disk.
This is in line with RocksDB's default durability.
Also, when dropped, the keyspace will try to persist the journal *to disk* synchronously.

## Multithreading, Async and Multiprocess

> !!! A single keyspace may **not** be loaded in parallel from separate *processes*.

However, Fjall is internally synchronized for multi-threaded access, so you can clone around the `Keyspace` and `Partition`s as needed, without needing to lock yourself.

For an async example, see the [`tokio`](https://github.com/fjall-rs/fjall/tree/main/examples/tokio) example.

## Feature flags

### bloom

Uses bloom filters to reduce disk I/O when serving point reads, but increases memory usage.

*Enabled by default.*

### lz4

Allows using `LZ4` compression, powered by [`lz4_flex`](https://github.com/PSeitz/lz4_flex).

*Enabled by default.*

### miniz

Allows using `DEFLATE/zlib` compression, powered by [`miniz_oxide`](https://github.com/Frommi/miniz_oxide).

*Disabled by default.*

### single_writer_tx

Allows opening a transactional Keyspace for single-writer (serialized) transactions, allowing RYOW (read-your-own-write), fetch-and-update and other atomic operations.

*Enabled by default.*

### ssi_tx

Allows opening a transactional Keyspace for multi-writer, serializable transactions, allowing RYOW (read-your-own-write), fetch-and-update and other atomic operations.
Conflict checking is done using optimistic concurrency control.

*Disabled by default.*

## Stable disk format

The disk format is stable as of 1.0.0.

2.0.0 uses a new disk format and needs a manual format migration.

Future breaking changes will result in a major version bump and a migration path.

For the underlying LSM-tree implementation, see: <https://crates.io/crates/lsm-tree>.

## Examples

[See here](https://github.com/fjall-rs/fjall/tree/main/examples) for practical examples.

And checkout [`Smoltable`](https://github.com/marvin-j97/smoltable), a standalone Bigtable-inspired mini wide-column database using `fjall` as its storage engine.

## Contributing

How can you help?

- [Ask a question](https://github.com/fjall-rs/fjall/discussions/new?category=q-a)
  - or join the Discord server: https://discord.com/invite/HvYGp4NFFk
- [Post benchmarks and things you created](https://github.com/fjall-rs/fjall/discussions/new?category=show-and-tell)
- [Open an issue](https://github.com/fjall-rs/fjall/issues/new) (bug report, weirdness)
- [Open a PR](https://github.com/fjall-rs/fjall/compare)

## License

All source code is licensed under MIT OR Apache-2.0.

All contributions are to be licensed as MIT OR Apache-2.0.
