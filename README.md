# LSM-tree

{badges}

A K.I.S.S. implementation of log-structured merge trees (LSM-trees/LSMTs).

```rs
use lsm_tree::{Config, Tree};

let folder = "data";

// A tree is a single physical keyspace/index/...
// and supports a BTreeMap-like API
// but all data is persisted to disk.
let tree = Config::new(folder).open()?;

tree.insert("my_key", "my_value")?;

let item = tree.get("my_key")?;
assert_eq!(Some("my_value".as_bytes().to_vec()), item);

// Flush to definitely make sure data is persisted
tree.flush()?;

// Search by prefix
for item in tree.prefix("prefix")?.into_iter() {
  // ...
}

// Search by range
for item in tree.range("a"..="z")?.into_iter() {
  // ...
}
```

## About

This is the fastest and most feature-rich LSM-tree implementation in Rust! It features, among other things:

- Block based tables with LZ4 compression
- Range & prefix searching with forward and reverse iteration
- Size-tiered or Levelled compaction with concurrency support
- Partitioned block index to reduce memory footprint and keep startup time minimal [1]
- Block caching to keep hot data in memory
- Bloom filters to avoid expensive disk access for non-existing items
- Sharded journal for concurrent writes
- Journal truncation on recovery for consistency
- Atomic write batches
- Snapshots (MVCC)
- Automatic background compaction
  - Does not spawn background threads unless actually needed
- Thread-safe (internally synchronized)
- 100% safe Rust

## Future

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

## License

All source code is MIT-licensed.

## Footnotes

[1] https://rocksdb.org/blog/2017/05/12/partitioned-index-filter.html
