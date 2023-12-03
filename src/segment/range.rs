use crate::block_cache::BlockCache;
use crate::Value;

use super::index::MetaIndex;
use super::reader::Reader;
use std::ops::Bound;
use std::ops::RangeBounds;
use std::path::Path;
use std::sync::Arc;

pub struct Range {
    iterator: Reader,
    range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
}

impl Range {
    pub fn new<P: AsRef<Path>>(
        path: P,
        segment_id: String,
        block_cache: Arc<BlockCache>,
        block_index: Arc<MetaIndex>,
        range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
    ) -> crate::Result<Self> {
        let offset_lo = match range.start_bound() {
            Bound::Unbounded => None,
            Bound::Included(start) | Bound::Excluded(start) => block_index
                .get_lower_bound_block_info(start)
                .map(|x| x.start_key),
        };

        let offset_hi = match range.end_bound() {
            Bound::Unbounded => None,
            Bound::Included(end) | Bound::Excluded(end) => block_index
                .get_upper_bound_block_info(end)
                .map(|x| x.start_key),
        };

        let iterator = Reader::new(
            path,
            segment_id,
            block_cache,
            block_index,
            offset_lo.as_ref(),
            offset_hi.as_ref(),
        )?;

        Ok(Self { iterator, range })
    }
}

impl Iterator for Range {
    type Item = crate::Result<Value>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let entry_result = self.iterator.next()?;

            match entry_result {
                Ok(entry) => {
                    match self.range.start_bound() {
                        Bound::Included(start) => {
                            if entry.key < *start {
                                // Before min key
                                continue;
                            }
                        }
                        Bound::Excluded(start) => {
                            if entry.key <= *start {
                                // Before or equal min key
                                continue;
                            }
                        }
                        Bound::Unbounded => {}
                    }

                    match self.range.end_bound() {
                        Bound::Included(start) => {
                            if entry.key > *start {
                                // After max key
                                return None;
                            }
                        }
                        Bound::Excluded(start) => {
                            if entry.key >= *start {
                                // Reached max key
                                return None;
                            }
                        }
                        Bound::Unbounded => {}
                    }

                    return Some(Ok(entry));
                }
                Err(error) => return Some(Err(error)),
            };
        }
    }
}

impl DoubleEndedIterator for Range {
    fn next_back(&mut self) -> Option<Self::Item> {
        loop {
            let entry_result = self.iterator.next_back()?;

            match entry_result {
                Ok(entry) => {
                    match self.range.start_bound() {
                        Bound::Included(start) => {
                            if entry.key < *start {
                                // Reached min key
                                return None;
                            }
                        }
                        Bound::Excluded(start) => {
                            if entry.key <= *start {
                                // Before min key
                                return None;
                            }
                        }
                        Bound::Unbounded => {}
                    }

                    match self.range.end_bound() {
                        Bound::Included(end) => {
                            if entry.key > *end {
                                // After max key
                                continue;
                            }
                        }
                        Bound::Excluded(end) => {
                            if entry.key >= *end {
                                // After or equal max key
                                continue;
                            }
                        }
                        Bound::Unbounded => {}
                    }

                    return Some(Ok(entry));
                }
                Err(error) => return Some(Err(error)),
            };
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        block_cache::BlockCache,
        segment::{
            index::MetaIndex,
            meta::Metadata,
            range::Range,
            writer::{Options, Writer},
        },
        Value,
    };
    use std::ops::{
        Bound::{self, *},
        RangeBounds,
    };
    use std::sync::Arc;
    use test_log::test;

    const ITEM_COUNT: u64 = 100_000;

    #[test]
    fn test_unbounded_range() -> crate::Result<()> {
        let folder = tempfile::tempdir()?.into_path();

        let mut writer = Writer::new(Options {
            path: folder.clone(),
            evict_tombstones: false,
            block_size: 4096,
        })?;

        let items = (0u64..ITEM_COUNT)
            .map(|i| Value::new(i.to_be_bytes(), nanoid::nanoid!(), false, 1000 + i));

        for item in items {
            writer.write(item)?;
        }

        writer.finish()?;

        let metadata = Metadata::from_writer(nanoid::nanoid!(), writer);
        metadata.write_to_file()?;

        let block_cache = Arc::new(BlockCache::new(usize::MAX));
        let meta_index = Arc::new(MetaIndex::from_file(
            metadata.id.clone(),
            &folder,
            Arc::clone(&block_cache),
        )?);

        {
            log::info!("Getting every item");

            let mut iter = Range::new(
                folder.join("blocks"),
                metadata.id.clone(),
                Arc::clone(&block_cache),
                Arc::clone(&meta_index),
                range_bounds_to_tuple(&..),
            )?;

            for key in (0u64..ITEM_COUNT).map(u64::to_be_bytes) {
                let item = iter.next().expect("item should exist")?;
                assert_eq!(key, &*item.key);
            }

            log::info!("Getting every item in reverse");

            let mut iter = Range::new(
                folder.join("blocks"),
                metadata.id.clone(),
                Arc::clone(&block_cache),
                Arc::clone(&meta_index),
                range_bounds_to_tuple(&..),
            )?;

            for key in (0u64..ITEM_COUNT).rev().map(u64::to_be_bytes) {
                let item = iter.next_back().expect("item should exist")?;
                assert_eq!(key, &*item.key);
            }
        }

        {
            log::info!("Getting every item (unbounded start)");

            let mut iter = Range::new(
                folder.join("blocks"),
                metadata.id.clone(),
                Arc::clone(&block_cache),
                Arc::clone(&meta_index),
                range_bounds_to_tuple(&..5_000_u64.to_be_bytes().to_vec()),
            )?;

            for key in (0..5_000).map(u64::to_be_bytes) {
                let item = iter.next().expect("item should exist")?;
                assert_eq!(key, &*item.key);
            }

            log::info!("Getting every item in reverse (unbounded start)");

            let mut iter = Range::new(
                folder.join("blocks"),
                metadata.id.clone(),
                Arc::clone(&block_cache),
                Arc::clone(&meta_index),
                range_bounds_to_tuple(&..5_000_u64.to_be_bytes().to_vec()),
            )?;

            for key in (1_000..5_000).rev().map(u64::to_be_bytes) {
                let item = iter.next_back().expect("item should exist")?;
                assert_eq!(key, &*item.key);
            }
        }

        {
            log::info!("Getting every item (unbounded end)");

            let mut iter = Range::new(
                folder.join("blocks"),
                metadata.id.clone(),
                Arc::clone(&block_cache),
                Arc::clone(&meta_index),
                range_bounds_to_tuple(&(1_000_u64.to_be_bytes().to_vec()..)),
            )?;

            for key in (1_000..5_000).map(u64::to_be_bytes) {
                let item = iter.next().expect("item should exist")?;
                assert_eq!(key, &*item.key);
            }

            log::info!("Getting every item in reverse (unbounded end)");

            let mut iter = Range::new(
                folder.join("blocks"),
                metadata.id,
                Arc::clone(&block_cache),
                Arc::clone(&meta_index),
                range_bounds_to_tuple(
                    &(1_000_u64.to_be_bytes().to_vec()..5_000_u64.to_be_bytes().to_vec()),
                ),
            )?;

            for key in (1_000..5_000).rev().map(u64::to_be_bytes) {
                let item = iter.next_back().expect("item should exist")?;
                assert_eq!(key, &*item.key);
            }
        }

        Ok(())
    }

    fn range_bounds_to_tuple<T: Clone>(range: &impl RangeBounds<T>) -> (Bound<T>, Bound<T>) {
        let start_bound = match range.start_bound() {
            Included(value) => Included(value.clone()),
            Excluded(value) => Excluded(value.clone()),
            Unbounded => Unbounded,
        };

        let end_bound = match range.end_bound() {
            Included(value) => Included(value.clone()),
            Excluded(value) => Excluded(value.clone()),
            Unbounded => Unbounded,
        };

        (start_bound, end_bound)
    }

    fn bounds_u64_to_bytes(bounds: &(Bound<u64>, Bound<u64>)) -> (Bound<Vec<u8>>, Bound<Vec<u8>>) {
        let start_bytes = match bounds.0 {
            Included(start) => Included(start.to_be_bytes().to_vec()),
            Excluded(start) => Excluded(start.to_be_bytes().to_vec()),
            Unbounded => Unbounded,
        };

        let end_bytes = match bounds.1 {
            Included(end) => Included(end.to_be_bytes().to_vec()),
            Excluded(end) => Excluded(end.to_be_bytes().to_vec()),
            Unbounded => Unbounded,
        };

        (start_bytes, end_bytes)
    }

    fn create_range(bounds: (Bound<u64>, Bound<u64>)) -> (u64, u64) {
        let start = match bounds.0 {
            Included(value) => value,
            Excluded(value) => value + 1,
            Unbounded => 0,
        };

        let end = match bounds.1 {
            Included(value) => value + 1,
            Excluded(value) => value,
            Unbounded => u64::MAX,
        };

        (start, end)
    }

    #[test]
    fn test_bounded_ranges() -> crate::Result<()> {
        let folder = tempfile::tempdir()?.into_path();

        let mut writer = Writer::new(Options {
            path: folder.clone(),
            evict_tombstones: false,
            block_size: 4096,
        })?;

        let items = (0u64..ITEM_COUNT)
            .map(|i| Value::new(i.to_be_bytes(), nanoid::nanoid!(), false, 1000 + i));

        for item in items {
            writer.write(item)?;
        }

        writer.finish()?;

        let metadata = Metadata::from_writer(nanoid::nanoid!(), writer);
        metadata.write_to_file()?;

        let block_cache = Arc::new(BlockCache::new(usize::MAX));
        let meta_index = Arc::new(MetaIndex::from_file(
            metadata.id.clone(),
            &folder,
            Arc::clone(&block_cache),
        )?);

        let ranges: Vec<(Bound<u64>, Bound<u64>)> = vec![
            range_bounds_to_tuple(&(0..1_000)),
            range_bounds_to_tuple(&(0..=1_000)),
            range_bounds_to_tuple(&(1_000..5_000)),
            range_bounds_to_tuple(&(1_000..=5_000)),
            range_bounds_to_tuple(&(1_000..ITEM_COUNT)),
            range_bounds_to_tuple(&..5_000),
        ];

        for bounds in ranges {
            log::info!("Bounds: {bounds:?}");

            let (start, end) = create_range(bounds);

            log::debug!("Getting every item in range");
            let range = std::ops::Range { start, end };

            let mut iter = Range::new(
                folder.join("blocks"),
                metadata.id.clone(),
                Arc::clone(&block_cache),
                Arc::clone(&meta_index),
                bounds_u64_to_bytes(&bounds),
            )?;

            for key in range.map(u64::to_be_bytes) {
                let item = iter.next().unwrap_or_else(|| {
                    panic!("item should exist: {:?} ({})", key, u64::from_be_bytes(key))
                })?;

                assert_eq!(key, &*item.key);
            }

            log::debug!("Getting every item in range in reverse");
            let range = std::ops::Range { start, end };

            let mut iter = Range::new(
                folder.join("blocks"),
                metadata.id.clone(),
                Arc::clone(&block_cache),
                Arc::clone(&meta_index),
                bounds_u64_to_bytes(&bounds),
            )?;

            for key in range.rev().map(u64::to_be_bytes) {
                let item = iter.next_back().unwrap_or_else(|| {
                    panic!("item should exist: {:?} ({})", key, u64::from_be_bytes(key))
                })?;

                assert_eq!(key, &*item.key);
            }
        }

        Ok(())
    }
}
