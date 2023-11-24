use super::{block::ValueBlock, index::MetaIndex};
use crate::{block_cache::BlockCache, Value};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    fs::File,
    io::{BufReader, Seek, SeekFrom},
    path::Path,
    sync::Arc,
};

#[allow(clippy::module_name_repetitions)]
/// Stupidly iterates through the entries of a segment
/// This does not account for tombstones
pub struct Reader {
    file_reader: BufReader<File>,
    block_index: Arc<MetaIndex>,

    segment_id: String,
    block_cache: Arc<BlockCache>,

    blocks: HashMap<Vec<u8>, VecDeque<Value>>,
    processed_blocks: HashSet<Vec<u8>>,

    current_lo: Option<Vec<u8>>,
    current_hi: Option<Vec<u8>>,
}

impl Reader {
    pub fn new<P: AsRef<Path>>(
        file: P,
        segment_id: String,
        block_cache: Arc<BlockCache>,
        block_index: Arc<MetaIndex>,
        start_offset: Option<&Vec<u8>>,
        end_offset: Option<&Vec<u8>>,
    ) -> crate::Result<Self> {
        let file_reader = BufReader::with_capacity(u16::MAX.into(), File::open(file)?);

        let mut iter = Self {
            file_reader,

            segment_id,
            block_cache,

            block_index,

            blocks: HashMap::with_capacity(2),
            processed_blocks: HashSet::with_capacity(100),

            current_lo: None,
            current_hi: None,
        };

        if let Some(offset) = start_offset {
            iter.current_lo = Some(offset.clone());
            iter.load_block(offset)?;
        }

        if let Some(offset) = end_offset {
            iter.current_hi = Some(offset.clone());

            if iter.current_lo != end_offset.cloned() {
                iter.load_block(offset)?;
            }
        }

        Ok(iter)
    }

    fn load_block(&mut self, key: &[u8]) -> crate::Result<Option<()>> {
        Ok(
            if let Some(block_ref) = self.block_index.get_lower_bound_block_info(key) {
                if let Some(block) = self
                    .block_cache
                    .get_disk_block(self.segment_id.clone(), &block_ref.start_key)
                {
                    // Cache hit: Copy from block
                    self.blocks.insert(key.to_vec(), block.items.clone().into());
                } else {
                    // Cache miss: load from disk

                    self.file_reader.seek(SeekFrom::Start(block_ref.offset))?;

                    let block =
                        ValueBlock::from_reader_compressed(&mut self.file_reader, block_ref.size)
                            .unwrap();

                    self.blocks.insert(key.to_vec(), block.items.into());
                }

                Some(())
            } else {
                None
            },
        )
    }
}

impl Iterator for Reader {
    type Item = crate::Result<Value>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_lo.is_none() {
            // Initialize first block
            let new_block_offset = self.block_index.get_first_block_key();
            self.current_lo = Some(new_block_offset.start_key.clone());

            if Some(&new_block_offset.start_key) == self.current_hi.as_ref() {
                // If the high bound is already at this block
                // Read from the block that was already loaded by hi
            } else if !self.processed_blocks.contains(&new_block_offset.start_key) {
                // Load first block for real, then take item from it
                let load_result = self.load_block(&new_block_offset.start_key);

                if let Err(error) = load_result {
                    return Some(Err(error));
                }
            }
        }

        if self.current_hi == self.current_lo && self.current_lo.is_some() {
            // We've reached the highest (last) block (bound by the hi marker)
            // Just consume from it instead
            let block = self
                .blocks
                .get_mut(&self.current_lo.as_ref().unwrap().clone());
            return block.and_then(VecDeque::pop_front).map(Ok);
        }

        if let Some(current_lo) = &self.current_lo {
            let block = self.blocks.get_mut(current_lo);

            return match block {
                Some(block) => {
                    let item = block.pop_front();

                    if block.is_empty() {
                        // Load next block
                        self.blocks.remove(current_lo);
                        self.processed_blocks.insert(current_lo.clone());

                        if let Some(new_block_offset) =
                            self.block_index.get_next_block_key(current_lo)
                        {
                            if !self.processed_blocks.contains(&new_block_offset.start_key) {
                                self.current_lo = Some(new_block_offset.start_key.clone());

                                if Some(&new_block_offset.start_key) == self.current_hi.as_ref() {
                                    // Do nothing
                                    // Next item consumed will use the existing higher block
                                } else {
                                    let load_result = self.load_block(&new_block_offset.start_key);
                                    if let Err(error) = load_result {
                                        return Some(Err(error));
                                    }
                                }
                            }
                        }
                    }

                    item.map(Ok)
                }
                None => None,
            };
        }

        None
    }
}

impl DoubleEndedIterator for Reader {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.current_hi.is_none() {
            // Initialize next block
            let new_block_offset = self.block_index.get_last_block_key();
            self.current_hi = Some(new_block_offset.start_key.clone());

            if Some(&new_block_offset.start_key) == self.current_lo.as_ref() {
                // If the low bound is already at this block
                // Read from the block that was already loaded by lo
            } else if !self.processed_blocks.contains(&new_block_offset.start_key) {
                // Load first block for real, then take item from it
                let load_result = self.load_block(&new_block_offset.start_key);
                if let Err(error) = load_result {
                    return Some(Err(error));
                }
            }
        }

        if self.current_hi == self.current_lo && self.current_hi.is_some() {
            // We've reached the lowest (first) block (bound by the lo marker)
            // Just consume from it instead
            let block = self
                .blocks
                .get_mut(&self.current_hi.as_ref().unwrap().clone());
            return block.and_then(VecDeque::pop_back).map(Ok);
        }

        if let Some(current_hi) = &self.current_hi {
            let block = self.blocks.get_mut(current_hi);

            return match block {
                Some(block) => {
                    let item = block.pop_back();

                    if block.is_empty() {
                        // Load next block
                        self.blocks.remove(current_hi);
                        self.processed_blocks.insert(current_hi.clone());

                        if let Some(new_block_offset) =
                            self.block_index.get_previous_block_key(current_hi)
                        {
                            if !self.processed_blocks.contains(&new_block_offset.start_key) {
                                self.current_hi = Some(new_block_offset.start_key.clone());
                                if Some(&new_block_offset.start_key) == self.current_lo.as_ref() {
                                    // Do nothing
                                    // Next item consumed will use the existing lower block
                                } else {
                                    let load_result = self.load_block(&new_block_offset.start_key);
                                    if let Err(error) = load_result {
                                        return Some(Err(error));
                                    }
                                }
                            }
                        }
                    }

                    item.map(Ok)
                }
                None => None,
            };
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        block_cache::BlockCache,
        segment::{
            index::MetaIndex,
            meta::Metadata,
            reader::Reader,
            writer::{Options, Writer},
        },
        Value,
    };
    use std::sync::Arc;
    use test_log::test;

    #[test]
    fn test_get_all() {
        const ITEM_COUNT: u64 = 100_000;

        let folder = tempfile::tempdir().unwrap().into_path();

        let mut writer = Writer::new(Options {
            path: folder.clone(),
            evict_tombstones: false,
            block_size: 4096,
        })
        .unwrap();

        let items = (0u64..ITEM_COUNT)
            .map(|i| Value::new(i.to_be_bytes(), nanoid::nanoid!(), false, 1000 + i));

        for item in items {
            writer.write(item).unwrap();
        }

        writer.finish().unwrap();

        let metadata = Metadata::from_writer(nanoid::nanoid!(), writer);
        metadata.write_to_file().unwrap();

        let block_cache = Arc::new(BlockCache::new(usize::MAX));
        let meta_index = Arc::new(
            MetaIndex::from_file(metadata.id.clone(), &folder, Arc::clone(&block_cache)).unwrap(),
        );

        log::info!("Getting every item");

        let mut iter = Reader::new(
            folder.join("blocks"),
            metadata.id.clone(),
            Arc::clone(&block_cache),
            Arc::clone(&meta_index),
            None,
            None,
        )
        .unwrap();

        for key in (0u64..ITEM_COUNT).map(u64::to_be_bytes) {
            let item = iter.next().unwrap().expect("item should exist");
            assert_eq!(key, &*item.key);
        }

        log::info!("Getting every item in reverse");

        let mut iter = Reader::new(
            folder.join("blocks"),
            metadata.id,
            Arc::clone(&block_cache),
            Arc::clone(&meta_index),
            None,
            None,
        )
        .unwrap();

        for key in (0u64..ITEM_COUNT).rev().map(u64::to_be_bytes) {
            let item = iter.next_back().unwrap().expect("item should exist");
            assert_eq!(key, &*item.key);
        }
    }
}
