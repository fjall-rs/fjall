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
use crate::{block_cache::BlockCache, value::SeqNo, Value};
use std::{
    fs::File,
    io::BufReader,
    ops::Bound,
    path::Path,
    sync::{Arc, Mutex},
};

/// Represents a `LSMT` segment (a.k.a. `SSTable`, `sorted string table`) that is located on disk.
/// A segment is an immutable list of key-value pairs, split into compressed blocks (see [`block::SegmentBlock`]).
/// The block offset and size in the file is saved in the "block index".
///
/// Deleted entries are represented by tombstones.
///
/// Segments can be merged together to remove duplicates, reducing disk space and improving read performance.
pub struct Segment {
    pub file: Mutex<BufReader<File>>,

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
    pub fn recover<P: AsRef<Path>>(folder: P, block_cache: Arc<BlockCache>) -> crate::Result<Self> {
        let metadata = Metadata::from_disk(folder.as_ref().join("meta.json"))?;
        let block_index = MetaIndex::from_file(
            metadata.id.clone(),
            folder.as_ref(),
            Arc::clone(&block_cache),
        )?;

        Ok(Self {
            file: Mutex::new(BufReader::new(File::open(folder.as_ref().join("blocks"))?)),
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
    pub fn get<K: AsRef<[u8]>>(
        &self,
        key: K,
        seqno: Option<SeqNo>,
    ) -> crate::Result<Option<Value>> {
        if !self.key_range_contains(&key) {
            return Ok(None);
        }

        // TODO: bloom

        let key = key.as_ref();

        let mut iter = self.prefix(key)?;

        iter.find(|x| match x {
            Ok(item) => seqno.map_or(true, |s| item.seqno < s),
            Err(_) => true,
        })
        .transpose()
    }

    /// Creates an iterator over the `Segment`.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn iter(&self) -> crate::Result<Reader> {
        let reader = Reader::new(
            self.metadata.path.join("blocks"),
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
    pub fn range(&self, range: (Bound<Vec<u8>>, Bound<Vec<u8>>)) -> crate::Result<Range> {
        let range = Range::new(
            self.metadata.path.join("blocks"),
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
    pub fn prefix<K: Into<Vec<u8>>>(&self, prefix: K) -> crate::Result<PrefixedReader> {
        let reader = PrefixedReader::new(
            self.metadata.path.join("blocks"),
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
