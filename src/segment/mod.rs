mod block;
pub mod index;
mod meta;
mod reader;
mod writer;

use crate::value::SeqNo;

use self::index::MetaIndex;
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
    // TODO: block cache
}

impl Segment {
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
