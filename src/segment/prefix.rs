use crate::Value;

use super::{index::MetaIndex, range::Range};
use std::{
    ops::Bound::{Excluded, Included, Unbounded},
    path::Path,
    sync::Arc,
};

#[allow(clippy::module_name_repetitions)]
pub struct PrefixedReader {
    iterator: Range,
    prefix: Vec<u8>,
}

impl PrefixedReader {
    pub fn new<P: AsRef<Path>, K: Into<Vec<u8>>>(
        path: P,
        //segment_id: String,
        block_index: Arc<MetaIndex>,
        //block_cache: Arc<BlockCache>,
        prefix: K,
    ) -> std::io::Result<Self> {
        let prefix = prefix.into();

        //TODO: optimize upper bound
        let upper_bound = block_index.get_prefix_upper_bound(&prefix);
        let upper_bound = upper_bound.map(|x| x.start_key).map_or(Unbounded, Excluded);

        let iterator = Range::new(
            path,
            //    segment_id,
            block_index,
            //   block_cache,
            (Included(prefix.clone()), upper_bound),
        )?;

        Ok(Self { iterator, prefix })
    }
}

impl Iterator for PrefixedReader {
    type Item = std::io::Result<Value>;

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
    fn test_lots_of_prefixed() {
        for item_count in [1, 10, 100, 1_000, 10_000] {
            let folder = tempfile::tempdir().unwrap().into_path();

            let mut writer = Writer::new(Options {
                path: folder.clone(),
                evict_tombstones: false,
                block_size: 4096,
            })
            .unwrap();

            for x in 0_u64..item_count {
                let item = Value::new(
                    {
                        let mut v = b"a/a/".to_vec();
                        v.extend_from_slice(&x.to_be_bytes());
                        v
                    },
                    nanoid::nanoid!(),
                    false,
                    0,
                );
                writer.write(item).unwrap();
            }

            for x in 0_u64..item_count {
                let item = Value::new(
                    {
                        let mut v = b"a/b/".to_vec();
                        v.extend_from_slice(&x.to_be_bytes());
                        v
                    },
                    nanoid::nanoid!(),
                    false,
                    0,
                );
                writer.write(item).unwrap();
            }

            for x in 0_u64..item_count {
                let item = Value::new(
                    {
                        let mut v = b"a/c/".to_vec();
                        v.extend_from_slice(&x.to_be_bytes());
                        v
                    },
                    nanoid::nanoid!(),
                    false,
                    0,
                );
                writer.write(item).unwrap();
            }

            writer.finalize().unwrap();

            let metadata = Metadata::from_writer(nanoid::nanoid!(), writer);
            metadata.write_to_file(&folder).unwrap();

            let meta_index = Arc::new(MetaIndex::from_file(&folder).unwrap());

            let iter =
                Reader::new(folder.join("blocks"), Arc::clone(&meta_index), None, None).unwrap();
            assert_eq!(iter.count() as u64, item_count * 3);

            let iter = PrefixedReader::new(
                folder.join("blocks"),
                Arc::clone(&meta_index),
                b"a/b/".to_vec(),
            )
            .unwrap();

            assert_eq!(iter.count() as u64, item_count);

            let iter = PrefixedReader::new(
                folder.join("blocks"),
                Arc::clone(&meta_index),
                b"a/b/".to_vec(),
            )
            .unwrap();

            assert_eq!(iter.rev().count() as u64, item_count);
        }
    }

    #[test]
    fn test_prefixed() {
        let folder = tempfile::tempdir().unwrap().into_path();

        let mut writer = Writer::new(Options {
            path: folder.clone(),
            evict_tombstones: false,
            block_size: 4096,
        })
        .unwrap();

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
        .map(|(idx, key)| Value::new(key, nanoid::nanoid!(), false, idx as SeqNo));

        for item in items {
            writer.write(item).unwrap();
        }

        writer.finalize().unwrap();

        let metadata = Metadata::from_writer(nanoid::nanoid!(), writer);
        metadata.write_to_file(&folder).unwrap();

        let meta_index = Arc::new(MetaIndex::from_file(&folder).unwrap());

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
            let iter =
                PrefixedReader::new(folder.join("blocks"), Arc::clone(&meta_index), prefix_key)
                    .unwrap();

            assert_eq!(iter.count(), item_count);
        }
    }
}
