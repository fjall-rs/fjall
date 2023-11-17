pub mod writer;

use super::block::ValueBlock;
use crate::disk_block::{DiskBlock, Error as DiskBlockError};
use crate::disk_block_index::{DiskBlockIndex, DiskBlockReference};
use crate::serde::{Deserializable, Serializable};
use crate::Value;
use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct BlockIndexEntry {
    pub offset: u64,
    pub size: u32,
    pub start_key: Vec<u8>,
}

impl Serializable for BlockIndexEntry {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), crate::SerializeError> {
        writer.write_all(&self.offset.to_be_bytes())?;
        writer.write_all(&self.size.to_be_bytes())?;

        // Write key length and key

        // NOTE: Truncation is okay and actually needed
        #[allow(clippy::cast_possible_truncation)]
        writer.write_all(&(self.start_key.len() as u16).to_be_bytes())?;
        writer.write_all(&self.start_key)?;

        Ok(())
    }
}

impl Deserializable for BlockIndexEntry {
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, crate::DeserializeError>
    where
        Self: Sized,
    {
        let mut offset_bytes = [0u8; std::mem::size_of::<u64>()];
        reader.read_exact(&mut offset_bytes)?;
        let offset = u64::from_be_bytes(offset_bytes);

        let mut size_bytes = [0u8; std::mem::size_of::<u32>()];
        reader.read_exact(&mut size_bytes)?;
        let size = u32::from_be_bytes(size_bytes);

        // Read key length
        let mut key_len_bytes = [0; std::mem::size_of::<u16>()];
        reader.read_exact(&mut key_len_bytes)?;
        let key_len = u16::from_be_bytes(key_len_bytes) as usize;

        // Read key
        let mut key = vec![0; key_len];
        reader.read_exact(&mut key)?;

        Ok(Self {
            offset,
            size,
            start_key: key,
        })
    }
}

// TODO: replace
use lockfree::map::Map;

pub struct MetaIndex {
    path: PathBuf,
    pub index: DiskBlockIndex,

    pub blocks: Map<Vec<u8>, Arc<DiskBlock<BlockIndexEntry>>>,
    // TODO: real block cache
    pub real_blocks: Map<Vec<u8>, Arc<ValueBlock>>,
}

impl DiskBlock<BlockIndexEntry> {
    pub(crate) fn get_lower_bound_block_info(&self, key: &[u8]) -> Option<&BlockIndexEntry> {
        self.items.iter().rev().find(|x| &*x.start_key <= key)
    }
}

impl MetaIndex {
    // TODO: need to use prefix iterator and get last
    pub fn get_latest(&self, key: &[u8]) -> Option<Value> {
        let (block_key, index_block_ref) = self.index.get_lower_bound_block_info(key)?;

        let index_block = match self.blocks.get(block_key) {
            Some(block) => block.1.clone(),
            None => {
                let block = DiskBlock::<BlockIndexEntry>::from_file_compressed(
                    self.path.join("index_blocks"),
                    index_block_ref.offset,
                    index_block_ref.size,
                )
                .unwrap(); // TODO:

                let block = Arc::new(block);

                self.blocks.insert(block_key.clone(), Arc::clone(&block));

                block
            }
        };

        let block_ref = index_block.get_lower_bound_block_info(key)?;

        let real_block = match self.real_blocks.get(&block_ref.start_key) {
            Some(block) => block.1.clone(),
            None => {
                let block = ValueBlock::from_file_compressed(
                    self.path.join("blocks"),
                    block_ref.offset,
                    block_ref.size,
                )
                .unwrap(); // TODO: panic

                let block: Arc<DiskBlock<Value>> = Arc::new(block);

                self.real_blocks
                    .insert(block_ref.start_key.clone(), Arc::clone(&block));

                block
            }
        };

        /* let real_block = ValueBlock::from_file_compressed(
            self.path.join("blocks"),
            block_ref.offset,
            block_ref.size,
        )
        .unwrap(); // TODO: panic */

        real_block.items.iter().find(|x| x.key == key).cloned()
    }

    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, DiskBlockError> {
        let size = std::fs::metadata(&path.as_ref().join("index"))?.len();
        let index = DiskBlock::<BlockIndexEntry>::from_file_compressed(
            path.as_ref().join("index"),
            0,
            size as u32,
        )?;

        let crc = DiskBlock::create_crc(&index.items);

        // TODO: no panic
        assert!(crc == index.crc, "crc fail");

        let mut tree = BTreeMap::new();

        for item in index.items {
            tree.insert(
                item.start_key,
                DiskBlockReference {
                    offset: item.offset,
                    size: item.size,
                },
            );
        }

        Ok(Self {
            path: path.as_ref().into(),
            index: DiskBlockIndex { data: tree },
            blocks: Map::default(),
            real_blocks: Map::default(),
        })
    }
}
