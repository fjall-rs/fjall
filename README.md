# LSM-tree

{badges}

A K.I.S.S. implementation of log-structured merge trees (LSM-trees/LSMTs).

```rs
use lsm_tree::{Config, Tree};

let folder = "data";
let tree = Config::new(folder).open()?;

tree.insert("my_key", "this is the actual value of the object")?;

let item = tree.get("my_key")?;
assert!(item.is_some());

// Flush to definitely make sure data is persisted
tree.flush()?;

// TODO: range & prefix
```

## About

This is the fastest and most feature-rich LSM-tree implementation in Rust! It features, among other things:

- Size-tiered or Levelled compaction with concurrency support
- Partitioned block index to reduce memory footprint and keep startup time minimal [1]
- Block caching to keep hot data in memory
- Bloom filters to avoid expensive disk access for non-existing items
- Sharded journal & memtable for concurrent writes
- Journal truncation on recovery for consistency
- Atomic write batches
- Automatic background compaction & tombstone eviction
  - Does not spawn background threads unless actually needed
- Thread-safe (internally synchronized)
- LZ4-compresses data
- CRChecks data blocks
- 100% safe Rust

## Benchmarks

Testing system:
- i7 7700k
- 24 GB RAM
- Linux (Ubuntu)
- M.2 SSD

{Add graphs here}

## License

All source code is MIT-licensed.

## Footnotes

[1] https://rocksdb.org/blog/2017/05/12/partitioned-index-filter.html
