use super::index::BlockIndex;
use super::reader::Reader;
use crate::block_cache::BlockCache;
use crate::descriptor_table::FileDescriptorTable;
use crate::value::UserKey;
use crate::Value;
use std::ops::Bound;
use std::ops::RangeBounds;
use std::sync::Arc;

pub struct Range {
    descriptor_table: Arc<FileDescriptorTable>,
    block_index: Arc<BlockIndex>,
    block_cache: Arc<BlockCache>,
    segment_id: Arc<str>,

    range: (Bound<UserKey>, Bound<UserKey>),

    iterator: Option<Reader>,
}

impl Range {
    pub fn new(
        descriptor_table: Arc<FileDescriptorTable>,
        segment_id: Arc<str>,
        block_cache: Arc<BlockCache>,
        block_index: Arc<BlockIndex>,
        range: (Bound<UserKey>, Bound<UserKey>),
    ) -> Self {
        Self {
            descriptor_table,
            block_cache,
            block_index,
            segment_id,

            iterator: None,
            range,
        }
    }

    fn initialize(&mut self) -> crate::Result<()> {
        let offset_lo = match self.range.start_bound() {
            Bound::Unbounded => None,
            Bound::Included(start) | Bound::Excluded(start) => self
                .block_index
                .get_lower_bound_block_info(start)?
                .map(|x| x.start_key),
        };

        let offset_hi = match self.range.end_bound() {
            Bound::Unbounded => None,
            Bound::Included(end) | Bound::Excluded(end) => self
                .block_index
                .get_upper_bound_block_info(end)?
                .map(|x| x.start_key),
        };

        let reader = Reader::new(
            self.descriptor_table.clone(),
            self.segment_id.clone(),
            Some(self.block_cache.clone()),
            self.block_index.clone(),
            offset_lo.as_ref(),
            offset_hi.as_ref(),
        );
        self.iterator = Some(reader);

        Ok(())
    }
}

impl Iterator for Range {
    type Item = crate::Result<Value>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.iterator.is_none() {
            if let Err(e) = self.initialize() {
                return Some(Err(e));
            };
        }

        loop {
            let entry_result = self
                .iterator
                .as_mut()
                .expect("should be initialized")
                .next()?;

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
        if self.iterator.is_none() {
            if let Err(e) = self.initialize() {
                return Some(Err(e));
            };
        }

        loop {
            let entry_result = self
                .iterator
                .as_mut()
                .expect("should be initialized")
                .next_back()?;

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
        descriptor_table::FileDescriptorTable,
        file::BLOCKS_FILE,
        segment::{
            index::BlockIndex,
            meta::Metadata,
            range::Range,
            writer::{Options, Writer},
        },
        value::{UserKey, ValueType},
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
    #[allow(clippy::expect_used)]
    fn test_unbounded_range() -> crate::Result<()> {
        let folder = tempfile::tempdir()?.into_path();

        let mut writer = Writer::new(Options {
            path: folder.clone(),
            evict_tombstones: false,
            block_size: 4096,

            #[cfg(feature = "bloom")]
            bloom_fp_rate: 0.01,
        })?;

        let items = (0u64..ITEM_COUNT).map(|i| {
            Value::new(
                i.to_be_bytes(),
                nanoid::nanoid!().as_bytes(),
                1000 + i,
                ValueType::Value,
            )
        });

        for item in items {
            writer.write(item)?;
        }

        writer.finish()?;

        let metadata = Metadata::from_writer(nanoid::nanoid!().into(), writer)?;
        metadata.write_to_file()?;

        let table = Arc::new(FileDescriptorTable::new(512, 1));
        table.insert(metadata.path.join(BLOCKS_FILE), metadata.id.clone());

        let block_cache = Arc::new(BlockCache::with_capacity_bytes(u64::MAX));
        let block_index = Arc::new(BlockIndex::from_file(
            metadata.id.clone(),
            table.clone(),
            &folder,
            Arc::clone(&block_cache),
        )?);

        {
            log::info!("Getting every item");

            let mut iter = Range::new(
                table.clone(),
                metadata.id.clone(),
                Arc::clone(&block_cache),
                Arc::clone(&block_index),
                range_bounds_to_tuple(&..),
            );

            for key in (0u64..ITEM_COUNT).map(u64::to_be_bytes) {
                let item = iter.next().expect("item should exist")?;
                assert_eq!(key, &*item.key);
            }

            log::info!("Getting every item in reverse");

            let mut iter = Range::new(
                table.clone(),
                metadata.id.clone(),
                Arc::clone(&block_cache),
                Arc::clone(&block_index),
                range_bounds_to_tuple(&..),
            );

            for key in (0u64..ITEM_COUNT).rev().map(u64::to_be_bytes) {
                let item = iter.next_back().expect("item should exist")?;
                assert_eq!(key, &*item.key);
            }
        }

        {
            log::info!("Getting every item (unbounded start)");

            let end: Arc<[u8]> = 5_000_u64.to_be_bytes().into();

            let mut iter = Range::new(
                table.clone(),
                metadata.id.clone(),
                Arc::clone(&block_cache),
                Arc::clone(&block_index),
                range_bounds_to_tuple::<UserKey>(&..end),
            );

            for key in (0..5_000).map(u64::to_be_bytes) {
                let item = iter.next().expect("item should exist")?;
                assert_eq!(key, &*item.key);
            }

            log::info!("Getting every item in reverse (unbounded start)");

            let end: Arc<[u8]> = 5_000_u64.to_be_bytes().into();

            let mut iter = Range::new(
                table.clone(),
                metadata.id.clone(),
                Arc::clone(&block_cache),
                Arc::clone(&block_index),
                range_bounds_to_tuple(&..end),
            );

            for key in (1_000..5_000).rev().map(u64::to_be_bytes) {
                let item = iter.next_back().expect("item should exist")?;
                assert_eq!(key, &*item.key);
            }
        }

        {
            log::info!("Getting every item (unbounded end)");

            let start: Arc<[u8]> = 1_000_u64.to_be_bytes().into();

            let mut iter = Range::new(
                table.clone(),
                metadata.id.clone(),
                Arc::clone(&block_cache),
                Arc::clone(&block_index),
                range_bounds_to_tuple(&(start..)),
            );

            for key in (1_000..5_000).map(u64::to_be_bytes) {
                let item = iter.next().expect("item should exist")?;
                assert_eq!(key, &*item.key);
            }

            log::info!("Getting every item in reverse (unbounded end)");

            let start: Arc<[u8]> = 1_000_u64.to_be_bytes().into();
            let end: Arc<[u8]> = 5_000_u64.to_be_bytes().into();

            let mut iter = Range::new(
                table,
                metadata.id,
                Arc::clone(&block_cache),
                Arc::clone(&block_index),
                range_bounds_to_tuple(&(start..end)),
            );

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

    fn bounds_u64_to_bytes(bounds: &(Bound<u64>, Bound<u64>)) -> (Bound<UserKey>, Bound<UserKey>) {
        let start_bytes = match bounds.0 {
            Included(start) => Included(start.to_be_bytes().into()),
            Excluded(start) => Excluded(start.to_be_bytes().into()),
            Unbounded => Unbounded,
        };

        let end_bytes = match bounds.1 {
            Included(end) => Included(end.to_be_bytes().into()),
            Excluded(end) => Excluded(end.to_be_bytes().into()),
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

            #[cfg(feature = "bloom")]
            bloom_fp_rate: 0.01,
        })?;

        let items = (0u64..ITEM_COUNT).map(|i| {
            Value::new(
                i.to_be_bytes(),
                nanoid::nanoid!().as_bytes(),
                1000 + i,
                ValueType::Value,
            )
        });

        for item in items {
            writer.write(item)?;
        }

        writer.finish()?;

        let metadata = Metadata::from_writer(nanoid::nanoid!().into(), writer)?;
        metadata.write_to_file()?;

        let table = Arc::new(FileDescriptorTable::new(512, 1));
        table.insert(metadata.path.join(BLOCKS_FILE), metadata.id.clone());

        let block_cache = Arc::new(BlockCache::with_capacity_bytes(u64::MAX));
        let block_index = Arc::new(BlockIndex::from_file(
            metadata.id.clone(),
            table.clone(),
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
                table.clone(),
                metadata.id.clone(),
                Arc::clone(&block_cache),
                Arc::clone(&block_index),
                bounds_u64_to_bytes(&bounds),
            );

            for key in range.map(u64::to_be_bytes) {
                let item = iter.next().unwrap_or_else(|| {
                    panic!("item should exist: {:?} ({})", key, u64::from_be_bytes(key))
                })?;

                assert_eq!(key, &*item.key);
            }

            log::debug!("Getting every item in range in reverse");
            let range = std::ops::Range { start, end };

            let mut iter = Range::new(
                table.clone(),
                metadata.id.clone(),
                Arc::clone(&block_cache),
                Arc::clone(&block_index),
                bounds_u64_to_bytes(&bounds),
            );

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
