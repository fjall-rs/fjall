<p align="center">
  <img src="/logo.png" height="128">
</p>
<p align="center>
  (temporary logo)
</p>

// TODO: 0.3.0

<!-- TODO: split CI pipelines, add badge here -->

[![docs.rs](https://img.shields.io/docsrs/fjall?color=green)](https://docs.rs/fjall)
[![Crates.io](https://img.shields.io/crates/v/fjall?color=blue)](https://crates.io/crates/fjall)
![MSRV](https://img.shields.io/badge/MSRV-1.74.0-blue)

Fjall is an LSM-based embedded key-value storage engine written in Rust.

It is not:

- a standalone server
- a relational database
- a wide-column database, it has no notions of columns

## Basic usage

```bash
cargo add fjall
```

```rs
TODO:
```

## About

- Thread-safe BTreeMap-like API
- 100% safe & stable Rust
- Range & prefix searching with forward and reverse iteration
- Block-based tables with LZ4 compression
- Size-tiered, (concurrent) Levelled and FIFO compaction strategies
- Partitioned block index to reduce memory footprint and keep startup time minimal [1]
- Block caching to keep hot data in memory
- Sharded journal for concurrent writes
- Journal truncation on recovery for consistency
- Column families (partitions) with cross-partition atomic semantics (atomic write batches)
- Cross-partition snapshots (MVCC)
- Automatic background compaction

For the underlying LSM-tree implementation, see: https://github.com/marvin-j97/fjall/tree/main/lsm-tree.

## Stable disk format

// TODO: 0.3.0

Is the disk format stable yet? Once 1.0.0 is released.
From that point onwards, breaking disk format changes will result
in a major bump.

## Examples

[See here](https://github.com/marvin-j97/fjall/tree/main/examples) for practical examples.

And checkout [`Smoltable`](https://github.com/marvin-j97/smoltable), a standalone Bigtable-inspired mini wide-column database using `fjall` as its storage engine.

## Minimum supported rust version (MSRV)

1.74.0

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
