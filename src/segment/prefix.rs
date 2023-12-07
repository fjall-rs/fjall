use super::{index::MetaIndex, range::Range};
use crate::{
    block_cache::BlockCache, descriptor_table::FileDescriptorTable, value::UserKey, Value,
};
use std::{
    ops::Bound::{Excluded, Included, Unbounded},
    sync::Arc,
};

#[allow(clippy::module_name_repetitions)]
pub struct PrefixedReader {
    iterator: Range,
    prefix: UserKey,
}

impl PrefixedReader {
    pub fn new<K: Into<UserKey>>(
        descriptor_table: Arc<FileDescriptorTable>,
        segment_id: String,
        block_cache: Arc<BlockCache>,
        block_index: Arc<MetaIndex>,
        prefix: K,
    ) -> crate::Result<Self> {
        let prefix = prefix.into();

        let upper_bound = block_index.get_prefix_upper_bound(&prefix)?;
        let upper_bound = upper_bound.map(|x| x.start_key).map_or(Unbounded, Excluded);

        let iterator = Range::new(
            descriptor_table,
            segment_id,
            block_cache,
            block_index,
            (Included(prefix.clone()), upper_bound),
        )?;

        Ok(Self { iterator, prefix })
    }
}

impl Iterator for PrefixedReader {
    type Item = crate::Result<Value>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let entry_result = self.iterator.next()?;

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
        loop {
            let entry_result = self.iterator.next_back()?;

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
            index::MetaIndex,
            meta::Metadata,
            prefix::PrefixedReader,
            reader::Reader,
            writer::{Options, Writer},
        },
        value::SeqNo,
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
            })?;

            for x in 0_u64..item_count {
                let item = Value::new(
                    {
                        let mut v = b"a/a/".to_vec();
                        v.extend_from_slice(&x.to_be_bytes());
                        v
                    },
                    nanoid::nanoid!().as_bytes(),
                    false,
                    0,
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
                    false,
                    0,
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
                    false,
                    0,
                );
                writer.write(item)?;
            }

            writer.finish()?;

            let metadata = Metadata::from_writer(nanoid::nanoid!(), writer)?;
            metadata.write_to_file()?;

            let block_cache = Arc::new(BlockCache::new(usize::MAX));
            let meta_index = Arc::new(MetaIndex::from_file(
                metadata.id.clone(),
                Arc::new(FileDescriptorTable::new(folder.join(BLOCKS_FILE))?),
                &folder,
                Arc::clone(&block_cache),
            )?);

            let iter = Reader::new(
                Arc::new(FileDescriptorTable::new(folder.join(BLOCKS_FILE))?),
                metadata.id.clone(),
                Arc::clone(&block_cache),
                Arc::clone(&meta_index),
                None,
                None,
            )?;
            assert_eq!(iter.count() as u64, item_count * 3);

            let iter = PrefixedReader::new(
                Arc::new(FileDescriptorTable::new(folder.join(BLOCKS_FILE))?),
                metadata.id.clone(),
                Arc::clone(&block_cache),
                Arc::clone(&meta_index),
                b"a/b/".to_vec(),
            )?;

            assert_eq!(iter.count() as u64, item_count);

            let iter = PrefixedReader::new(
                Arc::new(FileDescriptorTable::new(folder.join(BLOCKS_FILE))?),
                metadata.id.clone(),
                Arc::clone(&block_cache),
                Arc::clone(&meta_index),
                b"a/b/".to_vec(),
            )?;

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
        .map(|(idx, key)| Value::new(key, nanoid::nanoid!().as_bytes(), false, idx as SeqNo));

        for item in items {
            writer.write(item)?;
        }

        writer.finish()?;

        let metadata = Metadata::from_writer(nanoid::nanoid!(), writer)?;
        metadata.write_to_file()?;

        let block_cache = Arc::new(BlockCache::new(usize::MAX));
        let meta_index = Arc::new(MetaIndex::from_file(
            metadata.id.clone(),
            Arc::new(FileDescriptorTable::new(folder.join(BLOCKS_FILE))?),
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
                Arc::new(FileDescriptorTable::new(folder.join(BLOCKS_FILE))?),
                metadata.id.clone(),
                Arc::clone(&block_cache),
                Arc::clone(&meta_index),
                prefix_key,
            )?;

            assert_eq!(iter.count(), item_count);
        }

        Ok(())
    }
}
