use super::{index::BlockIndex, range::Range};
use crate::{
    block_cache::BlockCache, descriptor_table::FileDescriptorTable, value::UserKey, Value,
};
use std::{
    ops::Bound::{Excluded, Included, Unbounded},
    sync::Arc,
};

#[allow(clippy::module_name_repetitions)]
pub struct PrefixedReader {
    descriptor_table: Arc<FileDescriptorTable>,
    block_index: Arc<BlockIndex>,
    block_cache: Arc<BlockCache>,
    segment_id: Arc<str>,

    prefix: UserKey,

    iterator: Option<Range>,
}

impl PrefixedReader {
    pub fn new<K: Into<UserKey>>(
        descriptor_table: Arc<FileDescriptorTable>,
        segment_id: Arc<str>,
        block_cache: Arc<BlockCache>,
        block_index: Arc<BlockIndex>,
        prefix: K,
    ) -> Self {
        Self {
            block_cache,
            block_index,
            descriptor_table,
            segment_id,

            iterator: None,

            prefix: prefix.into(),
        }
    }

    fn initialize(&mut self) -> crate::Result<()> {
        let upper_bound = self.block_index.get_prefix_upper_bound(&self.prefix)?;
        let upper_bound = upper_bound.map(|x| x.start_key).map_or(Unbounded, Excluded);

        let iterator = Range::new(
            self.descriptor_table.clone(),
            self.segment_id.clone(),
            self.block_cache.clone(),
            self.block_index.clone(),
            (Included(self.prefix.clone()), upper_bound),
        );
        self.iterator = Some(iterator);

        Ok(())
    }
}

impl Iterator for PrefixedReader {
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
                    if entry.key < self.prefix {
                        // Before prefix key
                        continue;
                    }

                    if !entry.key.starts_with(&self.prefix) {
                        // Reached max key
                        return None;
                    }

                    return Some(Ok(entry));
                }
                Err(error) => return Some(Err(error)),
            };
        }
    }
}

impl DoubleEndedIterator for PrefixedReader {
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
                    if entry.key < self.prefix {
                        // Reached min key
                        return None;
                    }

                    if !entry.key.starts_with(&self.prefix) {
                        continue;
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
            prefix::PrefixedReader,
            reader::Reader,
            writer::{Options, Writer},
        },
        value::{SeqNo, ValueType},
        Value,
    };
    use std::sync::Arc;
    use test_log::test;

    #[test]
    fn test_lots_of_prefixed() -> crate::Result<()> {
        for item_count in [1, 10, 100, 1_000, 10_000] {
            let folder = tempfile::tempdir()?.into_path();

            let mut writer = Writer::new(Options {
                path: folder.clone(),
                evict_tombstones: false,
                block_size: 4096,

                #[cfg(feature = "bloom")]
                bloom_fp_rate: 0.01,
            })?;

            for x in 0_u64..item_count {
                let item = Value::new(
                    {
                        let mut v = b"a/a/".to_vec();
                        v.extend_from_slice(&x.to_be_bytes());
                        v
                    },
                    nanoid::nanoid!().as_bytes(),
                    0,
                    ValueType::Value,
                );
                writer.write(item)?;
            }

            for x in 0_u64..item_count {
                let item = Value::new(
                    {
                        let mut v = b"a/b/".to_vec();
                        v.extend_from_slice(&x.to_be_bytes());
                        v
                    },
                    nanoid::nanoid!().as_bytes(),
                    0,
                    ValueType::Value,
                );
                writer.write(item)?;
            }

            for x in 0_u64..item_count {
                let item = Value::new(
                    {
                        let mut v = b"a/c/".to_vec();
                        v.extend_from_slice(&x.to_be_bytes());
                        v
                    },
                    nanoid::nanoid!().as_bytes(),
                    0,
                    ValueType::Value,
                );
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

            let iter = Reader::new(
                table.clone(),
                metadata.id.clone(),
                Some(Arc::clone(&block_cache)),
                Arc::clone(&block_index),
                None,
                None,
            );
            assert_eq!(iter.count() as u64, item_count * 3);

            let iter = PrefixedReader::new(
                table.clone(),
                metadata.id.clone(),
                Arc::clone(&block_cache),
                Arc::clone(&block_index),
                b"a/b/".to_vec(),
            );

            assert_eq!(iter.count() as u64, item_count);

            let iter = PrefixedReader::new(
                table,
                metadata.id.clone(),
                Arc::clone(&block_cache),
                Arc::clone(&block_index),
                b"a/b/".to_vec(),
            );

            assert_eq!(iter.rev().count() as u64, item_count);
        }

        Ok(())
    }

    #[test]
    fn test_prefixed() -> crate::Result<()> {
        let folder = tempfile::tempdir()?.into_path();

        let mut writer = Writer::new(Options {
            path: folder.clone(),
            evict_tombstones: false,
            block_size: 4096,

            #[cfg(feature = "bloom")]
            bloom_fp_rate: 0.01,
        })?;

        let items = [
            b"a".to_vec(),
            b"a/a".to_vec(),
            b"a/b".to_vec(),
            b"a/b/a".to_vec(),
            b"a/b/z".to_vec(),
            b"a/z/a".to_vec(),
            b"aaa".to_vec(),
            b"aaa/a".to_vec(),
            b"aaa/z".to_vec(),
            b"b/a".to_vec(),
            b"b/b".to_vec(),
        ]
        .into_iter()
        .enumerate()
        .map(|(idx, key)| {
            Value::new(
                key,
                nanoid::nanoid!().as_bytes(),
                idx as SeqNo,
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

        let expected = [
            (b"a".to_vec(), 9),
            (b"a/".to_vec(), 5),
            (b"b".to_vec(), 2),
            (b"b/".to_vec(), 2),
            (b"a".to_vec(), 9),
            (b"a/".to_vec(), 5),
            (b"b".to_vec(), 2),
            (b"b/".to_vec(), 2),
        ];

        for (prefix_key, item_count) in expected {
            let iter = PrefixedReader::new(
                table.clone(),
                metadata.id.clone(),
                Arc::clone(&block_cache),
                Arc::clone(&block_index),
                prefix_key,
            );

            assert_eq!(iter.count(), item_count);
        }

        Ok(())
    }
}
