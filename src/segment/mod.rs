pub mod block;
pub mod index;
pub mod meta;
mod prefix;
mod range;
mod reader;
pub mod writer;

use self::{
    block::ValueBlock, index::MetaIndex, prefix::PrefixedReader, range::Range, reader::Reader,
};
use crate::{block_cache::BlockCache, value::SeqNo, Value};
use std::{ops::Bound, sync::Arc};

/// Represents a `LSMT` segment (a.k.a. `SSTable`, `sorted string table`) that is located on disk.
/// A segment is an immutable list of key-value pairs, split into compressed blocks (see [`block::SegmentBlock`]).
/// The block offset and size in the file is saved in the "block index".
///
/// Deleted entries are represented by tombstones.
///
/// Segments can be merged together to remove duplicates, reducing disk space and improving read performance.
pub struct Segment {
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
    /// Retrieves an item from the segment
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs
    pub fn get(&self, key: &[u8]) -> Option<Value> {
        let block_ref = self.block_index.get_latest(key)?;

        let real_block = match self.block_cache.get_disk_block(&block_ref.start_key) {
            Some(block) => block,
            None => {
                let block = ValueBlock::from_file_compressed(
                    self.metadata.path.join("blocks"),
                    block_ref.offset,
                    block_ref.size,
                )
                .unwrap(); // TODO: panic

                let block = Arc::new(block);

                self.block_cache
                    .insert_disk_block(block_ref.start_key.clone(), Arc::clone(&block));

                block
            }
        };

        let item_index = real_block.items.binary_search_by(|x| (*x.key).cmp(key));
        item_index.map_or(None, |idx| Some(real_block.items[idx].clone()))
    }

    /// Counts all items in the segment
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs
    pub fn len(&self) -> std::io::Result<usize> {
        Ok(Reader::new(
            self.metadata.path.join("blocks"),
            /* self.metadata.id.clone(),
            Arc::clone(&self.block_index), */
            Arc::clone(&self.block_index),
            None,
            None,
        )?
        .count())
    }

    /// Creates an iterator over the `Segment`
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs
    pub fn iter(&self) -> std::io::Result<Reader> {
        Reader::new(
            self.metadata.path.join("blocks"),
            /* self.metadata.id.clone(),
            Arc::clone(&self.block_index), */
            Arc::clone(&self.block_index),
            None,
            None,
        )
    }

    /// Creates a ranged iterator over the `Segment`
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs
    pub fn range(&self, range: (Bound<Vec<u8>>, Bound<Vec<u8>>)) -> std::io::Result<Range> {
        Range::new(
            self.metadata.path.join("blocks"),
            //self.metadata.id.clone(),
            Arc::clone(&self.block_index),
            //Arc::clone(&self.block_cache),
            range,
        )
    }

    /// Creates a prefixed iterator over the `Segment`
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs
    pub fn prefix<K: Into<Vec<u8>>>(&self, prefix: K) -> std::io::Result<PrefixedReader> {
        PrefixedReader::new(
            self.metadata.path.join("blocks"),
            //self.metadata.id.clone(),
            Arc::clone(&self.block_index),
            //Arc::clone(&self.block_cache),
            prefix,
        )
    }

    /// Returns the highest sequence number in the segment
    pub fn lsn(&self) -> SeqNo {
        self.metadata.seqnos.1
    }

    /// Returns the amount of tombstone markers in the `Segment`
    pub fn tombstone_count(&self) -> u64 {
        self.metadata.tombstone_count
    }

    /// Returns true if the key is contained in the segment's key range
    pub(crate) fn key_range_contains<K: AsRef<[u8]>>(&self, key: K) -> bool {
        self.metadata.key_range_contains(key)
    }

    /// Checks if a key range is (partially or fully) contained in this segment
    pub(crate) fn check_key_range_overlap(
        &self,
        bounds: &(Bound<Vec<u8>>, Bound<Vec<u8>>),
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
