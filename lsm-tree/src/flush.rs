use crate::{
    descriptor_table::FileDescriptorTable,
    file::BLOCKS_FILE,
    memtable::MemTable,
    segment::{index::BlockIndex, meta::Metadata, writer::Writer, Segment},
    BlockCache,
};
use std::{path::PathBuf, sync::Arc};

#[cfg(feature = "bloom")]
use crate::bloom::BloomFilter;

#[cfg(feature = "bloom")]
use crate::file::BLOOM_FILTER_FILE;

/// Flush options
#[doc(hidden)]
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

    // Descriptor table
    pub descriptor_table: Arc<FileDescriptorTable>,
}

/// Flushes a memtable, creating a segment in the given folder
#[doc(hidden)]
pub fn flush_to_segment(opts: Options) -> crate::Result<Segment> {
    let segment_folder = opts.folder.join(&*opts.segment_id);
    log::debug!("Flushing segment to {}", segment_folder.display());

    let mut segment_writer = Writer::new(crate::segment::writer::Options {
        path: segment_folder.clone(),
        evict_tombstones: false,
        block_size: opts.block_size,

        #[cfg(feature = "bloom")]
        bloom_fp_rate: 0.0001,
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

    // TODO: if L0, L1, preload block index (non-partitioned)
    let block_index = Arc::new(BlockIndex::from_file(
        opts.segment_id.clone(),
        opts.descriptor_table.clone(),
        &segment_folder,
        opts.block_cache.clone(),
    )?);

    let created_segment = Segment {
        descriptor_table: opts.descriptor_table.clone(),
        metadata,
        block_index,
        block_cache: opts.block_cache,

        #[cfg(feature = "bloom")]
        bloom_filter: BloomFilter::from_file(segment_folder.join(BLOOM_FILTER_FILE))?,
    };

    opts.descriptor_table.insert(
        created_segment.metadata.path.join(BLOCKS_FILE),
        created_segment.metadata.id.clone(),
    );

    log::debug!("Flushed segment to {}", segment_folder.display());

    Ok(created_segment)
}
