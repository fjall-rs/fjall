[![CI](https://github.com/marvin-j97/lsm-tree/actions/workflows/test.yml/badge.svg)](https://github.com/marvin-j97/lsm-tree/actions/workflows/test.yml)


A K.I.S.S. implementation of log-structured merge trees (LSM-trees/LSMTs).

```rs
use lsm_tree::{Config, Tree};

let folder = "data";

// A tree is a single physical keyspace/index/...
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
for item in &tree.prefix("prefix")? {
  // ...
}

// Search by range
for item in &tree.range("a"..="z")? {
  // ...
}

// Iterators implement DoubleEndedIterator, so you can search backwards, too!
for item in tree.prefix("prefix")?.into_iter().rev() {
  // ...
}
```

```
cargo add lsm-tree
```

## About

This is the fastest and most feature-rich LSM-tree implementation in Rust! It features, among other things:

- Block-based tables with LZ4 compression
- Range & prefix searching with forward and reverse iteration
- Size-tiered, (concurrent) Levelled and FIFO compaction strategies
- Partitioned block index to reduce memory footprint and keep startup time minimal [1]
- Block caching to keep hot data in memory
- Sharded journal for concurrent writes
- Journal truncation on recovery for consistency
- Atomic write batches
- Snapshots (MVCC)
- Automatic background compaction
  - Does not spawn background threads unless actually needed
- Thread-safe (internally synchronized)
- 100% safe Rust

## Stable disk format

Is the disk format stable yet? Not quite, notably missing is:

- Finalize journal format
- Bloom filters
- Write a specification

## Future

- Bloom filters to avoid expensive disk access for non-existing items
- Range tombstones

## Benchmarks

Testing system:
- i7 7700k
- 24 GB RAM
- Linux (Ubuntu)
- M.2 SSD

{Add graphs here}

## Examples

[See here](https://github.com/marvin-j97/lsm-tree/tree/main/examples) for practical examples.

## Minimum supported rust version (MSRV)

1.74.0

## License

All source code is MIT-licensed.

## Footnotes

[1] https://rocksdb.org/blog/2017/05/12/partitioned-index-filter.html
