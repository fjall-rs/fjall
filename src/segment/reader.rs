use std::{
    collections::{HashMap, HashSet, VecDeque},
    fs::File,
    io::{BufReader, Seek, SeekFrom},
    path::Path,
    sync::Arc,
};

use crate::Value;

use super::{block::ValueBlock, index::MetaIndex};

#[allow(clippy::module_name_repetitions)]
/// Stupidly iterates through the entries of a segment
/// This does not account for tombstones
pub struct Reader {
    file_reader: BufReader<File>,
    //segment_id: String,
    block_index: Arc<MetaIndex>,
    // TODO: block_cache
    blocks: HashMap<Vec<u8>, VecDeque<Value>>,
    processed_blocks: HashSet<Vec<u8>>,

    current_lo: Option<Vec<u8>>,
    current_hi: Option<Vec<u8>>,
}

impl Reader {
    pub fn new<P: AsRef<Path>>(
        file: P,
        //segment_id: String,
        block_index: Arc<MetaIndex>,
        start_offset: &Option<Vec<u8>>,
        end_offset: &Option<Vec<u8>>,
    ) -> std::io::Result<Self> {
        let file_reader = BufReader::with_capacity(u16::MAX.into(), File::open(file)?);

        let mut iter = Self {
            file_reader,
            // segment_id,
            block_index,

            blocks: HashMap::with_capacity(2),
            processed_blocks: HashSet::with_capacity(100),

            current_lo: None,
            current_hi: None,
        };

        if let Some(ref offset) = start_offset {
            iter.current_lo = Some(offset.clone());
            iter.load_block(offset)?;
        }

        if let Some(ref offset) = end_offset {
            iter.current_hi = Some(offset.clone());
            if iter.current_lo != *end_offset {
                iter.load_block(offset)?;
            }
        }

        Ok(iter)
    }

    fn load_block(&mut self, key: &[u8]) -> std::io::Result<Option<()>> {
        Ok(if let Some(block_ref) = self.block_index.get_ref(key) {
            /* if let Some(block) = self.block_cache.get(&self.segment_id, block_ref.offset) {
                // Cache hit: Copy from block
                self.blocks.insert(key.to_vec(), block.items.clone());
            } else { */
            // Cache miss: load from disk

            self.file_reader.seek(SeekFrom::Start(block_ref.offset))?;
            let block = ValueBlock::from_reader_compressed(
                &mut self.file_reader,
                block_ref.offset,
                block_ref.size,
            )
            .unwrap();

            self.blocks.insert(key.to_vec(), block.items.into());

            /*   self.block_cache
            .insert(self.segment_id.clone(), block_ref.offset, Arc::new(block)); */
            /*  } */

            Some(())
        } else {
            None
        })
    }
}

impl Iterator for Reader {
    type Item = std::io::Result<Value>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_lo.is_none() {
            // Initialize first block
            let new_block_offset = self.block_index.get_first_block_key();
            self.current_lo = Some(new_block_offset.clone());

            if Some(&new_block_offset) == self.current_hi.as_ref() {
                // If the high bound is already at this block
                // Read from the block that was already loaded by hi
            } else if !self.processed_blocks.contains(&new_block_offset) {
                // Load first block for real, then take item from it
                let load_result = self.load_block(&new_block_offset);
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
                            if !self.processed_blocks.contains(&new_block_offset) {
                                self.current_lo = Some(new_block_offset.clone());
                                if Some(&new_block_offset) == self.current_hi.as_ref() {
                                    // Do nothing
                                    // Next item consumed will use the existing higher block
                                } else {
                                    let load_result = self.load_block(&new_block_offset);
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
            self.current_hi = Some(new_block_offset.clone());

            if Some(&new_block_offset) == self.current_lo.as_ref() {
                // If the low bound is already at this block
                // Read from the block that was already loaded by lo
            } else if !self.processed_blocks.contains(&new_block_offset) {
                // Load first block for real, then take item from it
                let load_result = self.load_block(&new_block_offset);
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
                            if !self.processed_blocks.contains(&new_block_offset) {
                                self.current_hi = Some(new_block_offset.clone());
                                if Some(&new_block_offset) == self.current_lo.as_ref() {
                                    // Do nothing
                                    // Next item consumed will use the existing lower block
                                } else {
                                    let load_result = self.load_block(&new_block_offset);
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
