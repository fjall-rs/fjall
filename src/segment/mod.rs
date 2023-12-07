pub mod block;
pub mod index;
pub mod meta;
pub mod prefix;
pub mod range;
pub mod reader;
pub mod writer;

use self::{
    index::MetaIndex, meta::Metadata, prefix::PrefixedReader, range::Range, reader::Reader,
};
use crate::{
    block_cache::BlockCache,
    descriptor_table::FileDescriptorTable,
    file::{BLOCKS_FILE, SEGMENT_METADATA_FILE},
    segment::reader::load_and_cache_by_block_handle,
    value::{SeqNo, UserKey},
    Value,
};
use std::{ops::Bound, path::Path, sync::Arc};

/// Represents a `LSMT` segment (a.k.a. `SSTable`, `sorted string table`) that is located on disk.
/// A segment is an immutable list of key-value pairs, split into compressed blocks (see [`block::SegmentBlock`]).
/// The block offset and size in the file is saved in the "block index".
///
/// Deleted entries are represented by tombstones.
///
/// Segments can be merged together to remove duplicates, reducing disk space and improving read performance.
pub struct Segment {
    pub descriptor_table: Arc<FileDescriptorTable>,

    /// Segment metadata object (will be stored in a JSON file)
    pub metadata: meta::Metadata,

    /// Translates key (first item of a block) to block offset (address inside file) and (compressed) size
    pub block_index: Arc<MetaIndex>,

    /// Block cache
    ///
    /// Stores index and data blocks
    pub block_cache: Arc<BlockCache>,
}

impl Segment {
    /// Tries to recover a segment from a folder.
    pub fn recover<P: AsRef<Path>>(
        folder: P,
        block_cache: Arc<BlockCache>,
        descriptor_table: Arc<FileDescriptorTable>,
    ) -> crate::Result<Self> {
        let folder = folder.as_ref();

        let metadata = Metadata::from_disk(folder.join(SEGMENT_METADATA_FILE))?;
        let block_index = MetaIndex::from_file(
            metadata.id.clone(),
            descriptor_table,
            folder,
            Arc::clone(&block_cache),
        )?;

        Ok(Self {
            descriptor_table: Arc::new(FileDescriptorTable::new(folder.join(BLOCKS_FILE))?),
            metadata,
            block_index: Arc::new(block_index),
            block_cache,
        })
    }

    /// Retrieves an item from the segment.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn get<K: AsRef<[u8]> + std::hash::Hash>(
        &self,
        key: K,
        seqno: Option<SeqNo>,
    ) -> crate::Result<Option<Value>> {
        if let Some(seqno) = seqno {
            if self.metadata.seqnos.0 >= seqno {
                return Ok(None);
            }
        }

        if !self.key_range_contains(&key) {
            return Ok(None);
        }

        // TODO: bloom filter query here

        let key = key.as_ref();

        match seqno {
            None => {
                // NOTE: Fastpath for non-seqno reads (which are most common)
                // This avoids setting up a rather expensive prefix reader
                // (see explanation for that below)
                // This only really works because sequence numbers are sorted
                // in descending order

                if let Some(block_handle) = self.block_index.get_latest(key.as_ref())? {
                    let block = load_and_cache_by_block_handle(
                        &self.descriptor_table,
                        &self.block_cache,
                        &self.metadata.id,
                        &block_handle,
                    )?;

                    let item = block.map_or_else(
                        || Ok(None),
                        |block| {
                            // TODO: maybe binary search can be used, but it needs to find the max seqno
                            Ok(block
                                .items
                                .iter()
                                .find(|item| item.key == key.as_ref().into())
                                .cloned())
                        },
                    );

                    item
                } else {
                    Ok(None)
                }
            }
            Some(seqno) => {
                //
                // TODO: maybe use a [Key..) range iterator with take_until(prefix_met)
                //       because we never reverse-iterate
                //       (which is something prefix-iter optimizes by setting the upper bound, which takes some time)
                //
                // NOTE: For finding a specific seqno,
                // we need to use a prefix reader
                // because nothing really prevents the version
                // we are searching for to be in the next block
                // after the one our key starts in
                //
                // Example (key:seqno), searching for a:2:
                //
                // [..., a:5, a:4] [a:3, a:2, b: 4, b:3]
                // ^               ^
                // Block A         Block B
                //
                // Based on get_lower_bound_block, "a" is in Block A
                // However, we are searching for A with seqno 2, which
                // unfortunately is in the next block
                let iter = self.prefix(key)?;

                for item in iter {
                    let item = item?;

                    if item.seqno < seqno {
                        return Ok(Some(item));
                    }
                }

                Ok(None)
            }
        }
    }

    /// Creates an iterator over the `Segment`.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn iter(&self) -> crate::Result<Reader> {
        let reader = Reader::new(
            Arc::clone(&self.descriptor_table),
            self.metadata.id.clone(),
            Arc::clone(&self.block_cache),
            Arc::clone(&self.block_index),
            None,
            None,
        )?;

        Ok(reader)
    }

    /// Creates a ranged iterator over the `Segment`.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn range(&self, range: (Bound<UserKey>, Bound<UserKey>)) -> crate::Result<Range> {
        let range = Range::new(
            Arc::clone(&self.descriptor_table),
            self.metadata.id.clone(),
            Arc::clone(&self.block_cache),
            Arc::clone(&self.block_index),
            range,
        )?;

        Ok(range)
    }

    /// Creates a prefixed iterator over the `Segment`.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn prefix<K: Into<UserKey>>(&self, prefix: K) -> crate::Result<PrefixedReader> {
        let reader = PrefixedReader::new(
            Arc::clone(&self.descriptor_table),
            self.metadata.id.clone(),
            Arc::clone(&self.block_cache),
            Arc::clone(&self.block_index),
            prefix,
        )?;

        Ok(reader)
    }

    /// Returns the highest sequence number in the segment.
    pub fn lsn(&self) -> SeqNo {
        self.metadata.seqnos.1
    }

    /// Returns the amount of tombstone markers in the `Segment`.
    pub fn tombstone_count(&self) -> u64 {
        self.metadata.tombstone_count
    }

    /// Returns `true` if the key is contained in the segment's key range.
    pub(crate) fn key_range_contains<K: AsRef<[u8]>>(&self, key: K) -> bool {
        self.metadata.key_range_contains(key)
    }

    /// Checks if a key range is (partially or fully) contained in this segment.
    pub(crate) fn check_key_range_overlap(
        &self,
        bounds: &(Bound<UserKey>, Bound<UserKey>),
    ) -> bool {
        let (lo, hi) = bounds;
        let (segment_lo, segment_hi) = &self.metadata.key_range;

        if *lo == Bound::Unbounded && *hi == Bound::Unbounded {
            return true;
        }

        if *hi == Bound::Unbounded {
            return match lo {
                Bound::Included(key) => key <= segment_hi,
                Bound::Excluded(key) => key < segment_hi,
                Bound::Unbounded => panic!("Invalid key range check"),
            };
        }

        if *lo == Bound::Unbounded {
            return match hi {
                Bound::Included(key) => key >= segment_lo,
                Bound::Excluded(key) => key > segment_lo,
                Bound::Unbounded => panic!("Invalid key range check"),
            };
        }

        let lo_included = match lo {
            Bound::Included(key) => key <= segment_hi,
            Bound::Excluded(key) => key < segment_hi,
            Bound::Unbounded => panic!("Invalid key range check"),
        };

        let hi_included = match hi {
            Bound::Included(key) => key >= segment_lo,
            Bound::Excluded(key) => key > segment_lo,
            Bound::Unbounded => panic!("Invalid key range check"),
        };

        lo_included && hi_included
    }
}
