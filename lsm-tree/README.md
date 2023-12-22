<p align="center">
  <img src="/lsm-tree/logo.png" height="128">
</p>

<!-- TODO: split CI pipelines, add badge here -->

[![docs.rs](https://img.shields.io/docsrs/lsm-tree?color=green)](https://docs.rs/lsm-tree)
[![Crates.io](https://img.shields.io/crates/v/lsm-tree?color=blue)](https://crates.io/crates/lsm-tree)
![MSRV](https://img.shields.io/badge/MSRV-1.74.0-blue)

A K.I.S.S. implementation of log-structured merge trees (LSM-trees/LSMTs) in Rust.

> This crate only provides the primitives for an LSM-tree.
> For example, it does not ship with a write-ahead log.
> You probably want to use https://github.com/marvin-j97/fjall instead.

## Basic usage

```bash
cargo add lsm-tree
```

```rs
use lsm_tree::{Tree, Config};

let folder = tempfile::tempdir()?;

// A tree is a single physical keyspace/index/...
// and supports a BTreeMap-like API
let tree = Config::new(folder).open()?;

// Note compared to the BTreeMap API, operations return a Result<T>
// So you can handle I/O errors if they occur
tree.insert("my_key", "my_value", /* sequence number */ 0);

let item = tree.get("my_key")?;
assert_eq!(Some("my_value".as_bytes().into()), item);

// Search by prefix
for item in &tree.prefix("prefix") {
  // ...
}

// Search by range
for item in &tree.range("a"..="z") {
  // ...
}

// Iterators implement DoubleEndedIterator, so you can search backwards, too!
for item in tree.prefix("prefix").into_iter().rev() {
  // ...
}

// Flush to secondary storage, clearing the memtable
// and persisting all in-memory data.
tree.flush_active_memtable().expect("should flush").join().unwrap()?;
assert_eq!(Some("my_value".as_bytes().into()), item);

// When some disk segments have amassed, use compaction
// to reduce the amount of disk segments

// (Choose compaction strategy based on workload)
use lsm_tree::compaction::Levelled;

let strategy = Levelled::default();
tree.compact(Box::new(strategy))?;

assert_eq!(Some("my_value".as_bytes().into()), item);
```

## About

This is the most feature-rich LSM-tree implementation in Rust! It features:

- Thread-safe BTreeMap-like API
- 100% safe & stable Rust
- Block-based tables with LZ4 compression
- Range & prefix searching with forward and reverse iteration
- Size-tiered, (concurrent) Levelled and FIFO compaction strategies
- Partitioned block index to reduce memory footprint and keep startup time tiny [1]
- Block caching to keep hot data in memory
- Snapshots (MVCC)

## Stable disk format

The disk format is stable as of 0.3.0.

Breaking changes will result in a major bump.

## Contributing

How can you help?

- [Ask a question](https://github.com/marvin-j97/fjall/discussions/new?category=q-a)
- [Post benchmarks and things you created](https://github.com/marvin-j97/fjall/discussions/new?category=show-and-tell)
- [Open an issue](https://github.com/marvin-j97/fjall/issues/new) (bug report, weirdness)
- [Open a PR](https://github.com/marvin-j97/fjall/compare)

All contributions are to be licensed as MIT OR Apache-2.0.

## License

All source code is licensed under MIT OR Apache-2.0.

## Footnotes

[1] https://rocksdb.org/blog/2017/05/12/partitioned-index-filter.html
