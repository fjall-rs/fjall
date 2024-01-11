use super::index::{block_handle::BlockHandle, BlockIndex};
use crate::{descriptor_table::FileDescriptorTable, disk_block::DiskBlock, BlockCache, Value};
use std::sync::Arc;

/// Value blocks are the building blocks of a [`crate::segment::Segment`]. Each block is a sorted list of [`Value`]s,
/// and stored in compressed form on disk, in sorted order.
///
/// The integrity of a block can be checked using the CRC value that is saved in it.
pub type ValueBlock = DiskBlock<Value>;

impl ValueBlock {
    pub fn size(&self) -> usize {
        std::mem::size_of::<Self>() + self.items.iter().map(Value::size).sum::<usize>()
    }
}

pub fn load_and_cache_by_block_handle(
    descriptor_table: &FileDescriptorTable,
    block_cache: &BlockCache,
    segment_id: &str,
    block_handle: &BlockHandle,
) -> crate::Result<Option<Arc<ValueBlock>>> {
    Ok(
        if let Some(block) = block_cache.get_disk_block(segment_id, &block_handle.start_key) {
            // Cache hit: Copy from block

            Some(block)
        } else {
            // Cache miss: load from disk

            let file_guard = descriptor_table
                .access(&segment_id.into())?
                .expect("should acquire file handle");

            let block = ValueBlock::from_file_compressed(
                &mut *file_guard.file.lock().expect("lock is poisoned"),
                block_handle.offset,
                block_handle.size,
            )?;

            drop(file_guard);

            let block = Arc::new(block);

            block_cache.insert_disk_block(
                segment_id.into(),
                block_handle.start_key.clone(),
                Arc::clone(&block),
            );

            Some(block)
        },
    )
}

pub fn load_and_cache_block_by_item_key<K: AsRef<[u8]>>(
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
