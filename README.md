<p align="center">
  <img src="/logo.png" height="128">
</p>

[![CI](https://github.com/marvin-j97/lsm-tree/actions/workflows/test.yml/badge.svg)](https://github.com/marvin-j97/lsm-tree/actions/workflows/test.yml)
[![docs.rs](https://img.shields.io/docsrs/lsm-tree?color=green)](https://docs.rs/lsm-tree)
[![Crates.io](https://img.shields.io/crates/v/lsm-tree?color=blue)](https://crates.io/crates/lsm-tree)
![MSRV](https://img.shields.io/badge/MSRV-1.74.0-blue)

A K.I.S.S. implementation of log-structured merge trees (LSM-trees/LSMTs) in Rust.

## Basic usage

```bash
cargo add lsm-tree
```

```rs
use lsm_tree::{Config, Tree};

let folder = "data";

// A tree is a single physical structure
// and supports a BTreeMap-like API
// but all data is persisted to disk.
let tree = Config::new(folder).open()?;

// Note compared to the BTreeMap API, operations return a Result<T>
// So you can handle I/O errors if they occur
tree.insert("my_key", "my_value")?;

let item = tree.get("my_key")?;
assert_eq!(Some("my_value".as_bytes().into()), item);

// Flush to definitely make sure data is persisted
tree.flush()?;

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

// Partitions are physically separate keyspaces inside the tree
// which can be used to create indices or locality groups with atomic semantics
// across the entire tree.
//
// If you know RocksDB: partitions are equivalent to column families.
let partition = tree.partition("my-partition")?;
partition.insert("my_key", "my_value_in_another_index");

let item = partition.get("my_key")?;
assert_eq!(Some("my_value_in_another_index".as_bytes().into()), item);
```

## About

This is the most feature-rich LSM-tree implementation in Rust! It features:

- Thread-safe BTreeMap-like API
- 100% safe & stable Rust
- Range & prefix searching with forward and reverse iteration
- Block-based tables with LZ4 compression
- Size-tiered, (concurrent) Levelled and FIFO compaction strategies
- Partitioned block index to reduce memory footprint and keep startup time minimal [1]
- Block caching to keep hot data in memory
- Sharded journal for concurrent writes
- Journal truncation on recovery for consistency
- Atomic write batches
- Snapshots (MVCC)
- Automatic background compaction
  - Does not spawn background threads unless actually needed

## Stable disk format

Is the disk format stable yet? Not quite, but there aren't any plans
to change it now, and breaking changes will probably result in a
major bump. If the disk format is fully pinned by unit tests
(making it immutable for all 0.xx.x versions), this text will be updated.

## Examples

[See here](https://github.com/marvin-j97/lsm-tree/tree/main/examples) for practical examples.

And checkout [`Smoltable`](https://github.com/marvin-j97/smoltable), a Rust-based Bigtable-inspired mini wide-column database using `lsm-tree` as its storage engine.

## Minimum supported rust version (MSRV)

1.74.0

## Contributing

How can you help?

- [Ask a question](https://github.com/marvin-j97/lsm-tree/discussions/new?category=q-a)
- [Post benchmarks and things you created](https://github.com/marvin-j97/lsm-tree/discussions/new?category=show-and-tell)
- [Open an issue](https://github.com/marvin-j97/lsm-tree/issues/new) (bug report, weirdness)
- [Open a PR](https://github.com/marvin-j97/lsm-tree/compare)

All contributions are to be licensed as MIT OR Apache-2.0.

## License

All source code is licensed under MIT OR Apache-2.0.

## Footnotes

[1] https://rocksdb.org/blog/2017/05/12/partitioned-index-filter.html

[2] https://rocksdb.org/blog/2018/11/21/delete-range.html

[3] https://www.usenix.org/system/files/conference/fast16/fast16-papers-lu.pdf

[4] https://github.com/facebook/rocksdb/wiki/BlobDB
