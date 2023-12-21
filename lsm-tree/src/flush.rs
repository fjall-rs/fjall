use crate::{
    descriptor_table::FileDescriptorTable,
    file::BLOCKS_FILE,
    memtable::MemTable,
    segment::{index::BlockIndex, meta::Metadata, writer::Writer, Segment},
    BlockCache,
};
use std::{path::PathBuf, sync::Arc};

/// Flush options
pub struct Options {
    /// MemTable to flush
    pub memtable: Arc<MemTable>,

    /// Unique segment ID
    pub segment_id: Arc<str>,

    /// Base folder of segments
    ///
    /// The segment will be stored in {folder}/{segment_id}
    pub folder: PathBuf,

    /// Block size in bytes
    pub block_size: u32,

    // Block cache
    pub block_cache: Arc<BlockCache>,
}

/// Flushes a memtable, creating a segment in the given folder
pub(crate) fn flush_to_segment(opts: Options) -> crate::Result<Segment> {
    let segment_folder = opts.folder.join(&*opts.segment_id);
    log::debug!("Flushing segment to {}", segment_folder.display());

    let mut segment_writer = Writer::new(crate::segment::writer::Options {
        path: segment_folder.clone(),
        evict_tombstones: false,
        block_size: opts.block_size,
    })?;

    for entry in &opts.memtable.items {
        let key = entry.key();
        let value = entry.value();
        segment_writer.write(crate::Value::from(((key.clone()), value.clone())))?;
    }

    segment_writer.finish()?;

    let metadata = Metadata::from_writer(opts.segment_id.clone(), segment_writer)?;
    metadata.write_to_file()?;

    log::debug!("Finalized segment write at {}", segment_folder.display());

    let descriptor_table = Arc::new(FileDescriptorTable::new(metadata.path.join(BLOCKS_FILE))?);

    let block_index = Arc::new(BlockIndex::from_file(
        opts.segment_id,
        Arc::clone(&descriptor_table),
        &segment_folder,
        opts.block_cache.clone(),
    )?);

    let created_segment = Segment {
        descriptor_table,
        metadata,
        block_index,
        block_cache: opts.block_cache,
    };

    log::debug!("Flushed segment to {}", segment_folder.display());

    Ok(created_segment)
}
