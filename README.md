# LSM-tree

{badges}

A K.I.S.S. implementation of log-structured merge trees (LSM-trees/LSMTs).

This is the fastest and most feature-rich LSM-tree implementation in Rust! It features, among other things:

- Size-tiered or Levelled compaction
- Partitioned block index to reduce memory footprint
- Block caching to keep hot data in memory
- Bloom filters to avoid expensive disk access for non-existing items
- MONKEY-optimized bloom filter allocation to optimize the memory footprint of bloom filters in regards to the false positive rate
- Sharded log & memtable for concurrent writes
- Atomic batch operations
- Automatic background compaction & tombstone eviction
  - Does not spawn background threads unless actually needed
- Thread-safe (internally synchronized)
- LZ4-compresses data
- 100% safe Rust

#### Performance to expect roughly:

- Instantaneous startup, even for 500M+ objects, with low memory overhead (<10 MB for 500M+ objects)
- ~500k+ writes per second for a single writer thread
- ??? MB/s throughput
- <1μs reads for hot (cached) data (2M+ reads per second)
- ??? for cold data (??? reads per second)

## Benchmarks

Testing system:
- i7 7700k
- 24 GB RAM
- Linux (Ubuntu)
- M.2 SSD

{Add graphs here}

## License

All source code is MIT-licensed.
