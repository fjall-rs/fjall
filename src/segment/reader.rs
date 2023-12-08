use super::{
    block::ValueBlock,
    index::{block_handle::BlockHandle, BlockIndex},
};
use crate::{
    block_cache::BlockCache, descriptor_table::FileDescriptorTable, value::UserKey, Value,
};
use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

#[allow(clippy::module_name_repetitions)]
/// Stupidly iterates through the entries of a segment
/// This does not account for tombstones
pub struct Reader {
    descriptor_table: Arc<FileDescriptorTable>,
    block_index: Arc<BlockIndex>,

    segment_id: String,
    block_cache: Arc<BlockCache>,

    blocks: HashMap<UserKey, VecDeque<Value>>,
    current_lo: Option<UserKey>,
    current_hi: Option<UserKey>,
}

pub fn load_and_cache_by_block_handle(
    descriptor_table: &FileDescriptorTable,
    block_cache: &BlockCache,
    segment_id: &str,
    block_handle: &BlockHandle,
) -> crate::Result<Option<Arc<ValueBlock>>> {
    Ok(
        if let Some(block) =
            block_cache.get_disk_block(segment_id.to_string(), &block_handle.start_key)
        {
            // Cache hit: Copy from block

            Some(block)
        } else {
            // Cache miss: load from disk

            let mut file_reader = descriptor_table.access();

            let block = ValueBlock::from_file_compressed(
                &mut *file_reader,
                block_handle.offset,
                block_handle.size,
            )?;

            drop(file_reader);

            let block = Arc::new(block);

            block_cache.insert_disk_block(
                segment_id.to_string(),
                block_handle.start_key.clone(),
                Arc::clone(&block),
            );

            Some(block)
        },
    )
}

pub fn load_and_cache_block<K: AsRef<[u8]>>(
    descriptor_table: &FileDescriptorTable,
    block_index: &BlockIndex,
    block_cache: &BlockCache,
    segment_id: &str,
    item_key: K,
) -> crate::Result<Option<Arc<ValueBlock>>> {
    Ok(
        if let Some(block_handle) = block_index.get_lower_bound_block_info(item_key.as_ref())? {
            load_and_cache_by_block_handle(
                descriptor_table,
                block_cache,
                segment_id,
                &block_handle,
            )?
        } else {
            None
        },
    )
}

impl Reader {
    pub fn new(
        descriptor_table: Arc<FileDescriptorTable>,
        segment_id: String,
        block_cache: Arc<BlockCache>,
        block_index: Arc<BlockIndex>,
        start_offset: Option<&UserKey>,
        end_offset: Option<&UserKey>,
    ) -> crate::Result<Self> {
        let mut iter = Self {
            descriptor_table,

            segment_id,
            block_cache,

            block_index,

            blocks: HashMap::with_capacity(2),
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
            if let Some(block) = load_and_cache_block(
                &self.descriptor_table,
                &self.block_index,
                &self.block_cache,
                &self.segment_id,
                key,
            )? {
                let items = block.items.clone().into();
                self.blocks.insert(key.to_vec().into(), items);
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
            let new_block_offset = match self.block_index.get_first_block_key() {
                Ok(x) => x,
                Err(e) => return Some(Err(e)),
            };
            self.current_lo = Some(new_block_offset.start_key.clone());

            if Some(&new_block_offset.start_key) == self.current_hi.as_ref() {
                // If the high bound is already at this block
                // Read from the block that was already loaded by hi
            } else {
                let load_result = self.load_block(&new_block_offset.start_key);

                if let Err(error) = load_result {
                    return Some(Err(error));
                }
            }
        }

        if let Some(current_lo) = &self.current_lo {
            if self.current_hi == self.current_lo {
                // We've reached the highest (last) block (bound by the hi marker)
                // Just consume from it instead
                let block = self.blocks.get_mut(&current_lo.clone());
                return block.and_then(VecDeque::pop_front).map(Ok);
            }
        }

        if let Some(current_lo) = &self.current_lo {
            let block = self.blocks.get_mut(current_lo);

            return match block {
                Some(block) => {
                    let item = block.pop_front();

                    if block.is_empty() {
                        // Load next block
                        self.blocks.remove(current_lo);

                        if let Some(new_block_offset) =
                            match self.block_index.get_next_block_key(current_lo) {
                                Ok(x) => x,
                                Err(e) => return Some(Err(e)),
                            }
                        {
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
            let new_block_offset = match self.block_index.get_last_block_key() {
                Ok(x) => x,
                Err(e) => return Some(Err(e)),
            };
            self.current_hi = Some(new_block_offset.start_key.clone());

            if Some(&new_block_offset.start_key) == self.current_lo.as_ref() {
                // If the low bound is already at this block
                // Read from the block that was already loaded by lo
            } else {
                // Load first block for real, then take item from it
                let load_result = self.load_block(&new_block_offset.start_key);
                if let Err(error) = load_result {
                    return Some(Err(error));
                }
            }
        }

        if let Some(current_hi) = &self.current_hi {
            if self.current_hi == self.current_lo {
                // We've reached the lowest (first) block (bound by the lo marker)
                // Just consume from it instead
                let block = self.blocks.get_mut(&current_hi.clone());
                return block.and_then(VecDeque::pop_back).map(Ok);
            }
        }

        if let Some(current_hi) = &self.current_hi {
            let block = self.blocks.get_mut(current_hi);

            return match block {
                Some(block) => {
                    let item = block.pop_back();

                    if block.is_empty() {
                        // Load next block
                        self.blocks.remove(current_hi);

                        if let Some(new_block_offset) =
                            match self.block_index.get_previous_block_key(current_hi) {
                                Ok(x) => x,
                                Err(e) => return Some(Err(e)),
                            }
                        {
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
        descriptor_table::FileDescriptorTable,
        file::BLOCKS_FILE,
        segment::{
            index::BlockIndex,
            meta::Metadata,
            reader::Reader,
            writer::{Options, Writer},
        },
        value::ValueType,
        Value,
    };
    use std::sync::Arc;
    use test_log::test;

    // TODO: rev test with seqnos...

    #[test]
    fn test_get_all() -> crate::Result<()> {
        const ITEM_COUNT: u64 = 100_000;

        let folder = tempfile::tempdir()?.into_path();

        let mut writer = Writer::new(Options {
            path: folder.clone(),
            evict_tombstones: false,
            block_size: 4096,
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

        let metadata = Metadata::from_writer(nanoid::nanoid!(), writer)?;
        metadata.write_to_file()?;

        let block_cache = Arc::new(BlockCache::new(usize::MAX));
        let meta_index = Arc::new(BlockIndex::from_file(
            metadata.id.clone(),
            Arc::new(FileDescriptorTable::new(folder.join(BLOCKS_FILE))?),
            &folder,
            Arc::clone(&block_cache),
        )?);

        log::info!("Getting every item");

        let mut iter = Reader::new(
            Arc::new(FileDescriptorTable::new(folder.join(BLOCKS_FILE))?),
            metadata.id.clone(),
            Arc::clone(&block_cache),
            Arc::clone(&meta_index),
            None,
            None,
        )?;

        for key in (0u64..ITEM_COUNT).map(u64::to_be_bytes) {
            // NOTE: It's just a test
            #[allow(clippy::expect_used)]
            let item = iter.next().expect("item should exist")?;
            assert_eq!(key, &*item.key);
        }

        log::info!("Getting every item in reverse");

        let mut iter = Reader::new(
            Arc::new(FileDescriptorTable::new(folder.join(BLOCKS_FILE))?),
            metadata.id,
            Arc::clone(&block_cache),
            Arc::clone(&meta_index),
            None,
            None,
        )?;

        for key in (0u64..ITEM_COUNT).rev().map(u64::to_be_bytes) {
            // NOTE: It's just a test
            #[allow(clippy::expect_used)]
            let item = iter.next_back().expect("item should exist")?;
            assert_eq!(key, &*item.key);
        }

        Ok(())
    }
}
