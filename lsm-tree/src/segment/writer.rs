use super::{block::ValueBlock, meta::Metadata};
use crate::{
    file::BLOCKS_FILE,
    id::generate_segment_id,
    segment::index::writer::Writer as IndexWriter,
    serde::Serializable,
    value::{SeqNo, UserKey},
    Value,
};
use lz4_flex::compress_prepend_size;
use std::{
    fs::File,
    io::{BufWriter, Write},
    path::PathBuf,
    sync::Arc,
};

#[cfg(feature = "bloom")]
use crate::bloom::BloomFilter;

#[cfg(feature = "bloom")]
use crate::file::BLOOM_FILTER_FILE;

/// Like `Writer` but will rotate to a new segment, once a segment grows larger than `target_size`
///
/// This results in a sorted "run" of segments
#[allow(clippy::module_name_repetitions)]
pub struct MultiWriter {
    /// Target size of segments in bytes
    ///
    /// If a segment reaches the target size, a new one is started,
    /// resulting in a sorted "run" of segments
    pub target_size: u64,

    pub opts: Options,
    created_items: Vec<Metadata>,

    pub current_segment_id: Arc<str>,
    pub writer: Writer,
}

impl MultiWriter {
    /// Sets up a new `MultiWriter` at the given segments folder
    pub fn new(target_size: u64, opts: Options) -> crate::Result<Self> {
        let segment_id = generate_segment_id();

        let writer = Writer::new(Options {
            path: opts.path.join(&*segment_id),
            evict_tombstones: opts.evict_tombstones,
            block_size: opts.block_size,

            #[cfg(feature = "bloom")]
            bloom_fp_rate: opts.bloom_fp_rate,
        })?;

        Ok(Self {
            target_size,
            created_items: Vec::with_capacity(10),
            opts,
            current_segment_id: segment_id,
            writer,
        })
    }

    /// Flushes the current writer, stores its metadata, and sets up a new writer for the next segment
    fn rotate(&mut self) -> crate::Result<()> {
        log::debug!("Rotating segment writer");

        // Flush segment, and start new one
        self.writer.finish()?;

        let new_segment_id = generate_segment_id();

        let new_writer = Writer::new(Options {
            path: self.opts.path.join(&*new_segment_id),
            evict_tombstones: self.opts.evict_tombstones,
            block_size: self.opts.block_size,

            #[cfg(feature = "bloom")]
            bloom_fp_rate: self.opts.bloom_fp_rate,
        })?;

        let old_writer = std::mem::replace(&mut self.writer, new_writer);
        let old_segment_id = std::mem::replace(&mut self.current_segment_id, new_segment_id);

        if old_writer.item_count > 0 {
            let metadata = Metadata::from_writer(old_segment_id, old_writer)?;
            self.created_items.push(metadata);
        }

        Ok(())
    }

    /// Writes an item
    pub fn write(&mut self, item: Value) -> crate::Result<()> {
        self.writer.write(item)?;

        if self.writer.file_pos >= self.target_size {
            self.rotate()?;
        }

        Ok(())
    }

    /// Finishes the last segment, making sure all data is written durably
    ///
    /// Returns the metadata of created segments
    pub fn finish(mut self) -> crate::Result<Vec<Metadata>> {
        // Finish writer and consume it
        // Don't use `rotate` because that will start a new writer, creating unneeded, empty segments
        self.writer.finish()?;

        if self.writer.item_count > 0 {
            let metadata = Metadata::from_writer(self.current_segment_id, self.writer)?;
            self.created_items.push(metadata);
        }

        Ok(self.created_items)
    }
}

/// Serializes and compresses values into blocks and writes them to disk
///
/// Also takes care of creating the block index
pub struct Writer {
    pub opts: Options,

    block_writer: BufWriter<File>,
    index_writer: IndexWriter,
    chunk: ValueBlock,

    pub block_count: usize,
    pub item_count: usize,
    pub file_pos: u64,

    /// Only takes user data into account
    pub uncompressed_size: u64,

    pub first_key: Option<UserKey>,
    pub last_key: Option<UserKey>,
    pub tombstone_count: usize,
    pub chunk_size: usize,

    pub lowest_seqno: SeqNo,
    pub highest_seqno: SeqNo,

    pub key_count: usize,
    current_key: Option<UserKey>,

    /// Hashes for bloom filter
    ///
    /// using enhanced double hashing, so we got two u64s
    #[cfg(feature = "bloom")]
    bloom_hash_buffer: Vec<(u64, u64)>,
}

pub struct Options {
    pub path: PathBuf,
    pub evict_tombstones: bool,
    pub block_size: u32,

    #[cfg(feature = "bloom")]
    pub bloom_fp_rate: f32,
}

impl Writer {
    /// Sets up a new `MultiWriter` at the given segments folder
    pub fn new(opts: Options) -> crate::Result<Self> {
        std::fs::create_dir_all(&opts.path)?;

        let block_writer = File::create(opts.path.join(BLOCKS_FILE))?;
        let block_writer = BufWriter::with_capacity(512_000, block_writer);

        let index_writer = IndexWriter::new(&opts.path, opts.block_size)?;

        let chunk = ValueBlock {
            items: Vec::with_capacity(1_000),
            crc: 0,
        };

        Ok(Self {
            opts,

            block_writer,
            index_writer,
            chunk,

            block_count: 0,
            item_count: 0,
            file_pos: 0,
            uncompressed_size: 0,

            first_key: None,
            last_key: None,
            chunk_size: 0,
            tombstone_count: 0,

            lowest_seqno: SeqNo::MAX,
            highest_seqno: 0,

            current_key: None,
            key_count: 0,

            #[cfg(feature = "bloom")]
            bloom_hash_buffer: Vec::with_capacity(1_000),
        })
    }

    /// Writes a compressed block to disk
    ///
    /// This is triggered when a `Writer::write` causes the buffer to grow to the configured `block_size`
    fn write_block(&mut self) -> crate::Result<()> {
        debug_assert!(!self.chunk.items.is_empty());

        let uncompressed_chunk_size = self
            .chunk
            .items
            .iter()
            .map(|item| item.size() as u64)
            .sum::<u64>();

        self.uncompressed_size += uncompressed_chunk_size;

        // Serialize block
        let mut bytes = Vec::with_capacity(u16::MAX.into());
        self.chunk.crc = ValueBlock::create_crc(&self.chunk.items)?;
        self.chunk
            .serialize(&mut bytes)
            .expect("should serialize block");

        // Compress using LZ4
        let bytes = compress_prepend_size(&bytes);

        // Write to file
        self.block_writer.write_all(&bytes)?;

        // NOTE: Blocks are never bigger than 4 GB anyway,
        // so it's fine to just truncate it
        #[allow(clippy::cast_possible_truncation)]
        let bytes_written = bytes.len() as u32;

        // Expect is fine, because the chunk is not empty
        let first = self.chunk.items.first().expect("Chunk should not be empty");

        self.index_writer
            .register_block(first.key.clone(), self.file_pos, bytes_written)?;

        // Adjust metadata
        self.file_pos += u64::from(bytes_written);
        self.item_count += self.chunk.items.len();
        self.block_count += 1;
        self.chunk.items.clear();

        Ok(())
    }

    /// Writes an item
    pub fn write(&mut self, item: Value) -> crate::Result<()> {
        if item.is_tombstone() {
            if self.opts.evict_tombstones {
                return Ok(());
            }

            self.tombstone_count += 1;
        }

        #[cfg(feature = "bloom")]
        self.bloom_hash_buffer
            .push(BloomFilter::get_hash(&item.key));

        if Some(&item.key) != self.current_key.as_ref() {
            self.key_count += 1;
            self.current_key = Some(item.key.clone());
        }

        let item_key = item.key.clone();
        let seqno = item.seqno;

        self.chunk_size += item.size();
        self.chunk.items.push(item);

        if self.chunk_size >= self.opts.block_size as usize {
            self.write_block()?;
            self.chunk_size = 0;
        }

        if self.first_key.is_none() {
            self.first_key = Some(item_key.clone());
        }
        self.last_key = Some(item_key);

        if self.lowest_seqno > seqno {
            self.lowest_seqno = seqno;
        }

        if self.highest_seqno < seqno {
            self.highest_seqno = seqno;
        }

        Ok(())
    }

    /// Finishes the segment, making sure all data is written durably
    pub fn finish(&mut self) -> crate::Result<()> {
        if !self.chunk.items.is_empty() {
            self.write_block()?;
        }

        // No items written! Just delete segment folder and return nothing
        if self.item_count == 0 {
            log::debug!(
                "Deleting empty segment folder ({}) because no items were written",
                self.opts.path.display()
            );
            std::fs::remove_dir_all(&self.opts.path)?;
            return Ok(());
        }

        // First, flush all data blocks
        self.block_writer.flush()?;

        // Append index blocks to file
        self.index_writer.finish(self.file_pos)?;

        // Then fsync the blocks file
        self.block_writer.get_mut().sync_all()?;

        // NOTE: BloomFilter::write_to_file fsyncs internally
        #[cfg(feature = "bloom")]
        {
            let n = self.bloom_hash_buffer.len();
            log::debug!("Writing bloom filter with {n} hashes");

            let mut filter = BloomFilter::with_fp_rate(n, self.opts.bloom_fp_rate);

            for hash in std::mem::take(&mut self.bloom_hash_buffer) {
                filter.set_with_hash(hash);
            }

            filter.write_to_file(self.opts.path.join(BLOOM_FILTER_FILE))?;
        }

        #[cfg(not(target_os = "windows"))]
        {
            // fsync folder on Unix
            let folder = std::fs::File::open(&self.opts.path)?;
            folder.sync_all()?;
        }

        log::debug!(
            "Written {} items in {} blocks into new segment file, written {} MB",
            self.item_count,
            self.block_count,
            self.file_pos / 1024 / 1024
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::descriptor_table::FileDescriptorTable;
    use crate::value::ValueType;
    use crate::{
        block_cache::BlockCache,
        segment::{index::BlockIndex, meta::Metadata, reader::Reader},
        Value,
    };
    use std::sync::Arc;
    use test_log::test;

    #[test]
    fn test_write_and_read() -> crate::Result<()> {
        const ITEM_COUNT: u64 = 100;

        let folder = tempfile::tempdir()?.into_path();

        let mut writer = Writer::new(Options {
            path: folder.clone(),
            evict_tombstones: false,
            block_size: 4096,

            #[cfg(feature = "bloom")]
            bloom_fp_rate: 0.01,
        })?;

        let items = (0u64..ITEM_COUNT).map(|i| {
            Value::new(
                i.to_be_bytes(),
                nanoid::nanoid!().as_bytes(),
                0,
                ValueType::Value,
            )
        });

        for item in items {
            writer.write(item)?;
        }

        writer.finish()?;

        let metadata = Metadata::from_writer(nanoid::nanoid!().into(), writer)?;
        metadata.write_to_file()?;
        assert_eq!(ITEM_COUNT, metadata.item_count);
        assert_eq!(ITEM_COUNT, metadata.key_count);

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
            table,
            metadata.id,
            Some(Arc::clone(&block_cache)),
            Arc::clone(&block_index),
            None,
            None,
        );

        assert_eq!(ITEM_COUNT, iter.count() as u64);

        Ok(())
    }

    #[test]
    fn test_write_and_read_mvcc() -> crate::Result<()> {
        const ITEM_COUNT: u64 = 1_000;
        const VERSION_COUNT: u64 = 5;

        let folder = tempfile::tempdir()?.into_path();

        let mut writer = Writer::new(Options {
            path: folder.clone(),
            evict_tombstones: false,
            block_size: 4096,

            #[cfg(feature = "bloom")]
            bloom_fp_rate: 0.01,
        })?;

        for key in 0u64..ITEM_COUNT {
            for seqno in (0..VERSION_COUNT).rev() {
                let value = Value::new(
                    key.to_be_bytes(),
                    nanoid::nanoid!().as_bytes(),
                    seqno,
                    ValueType::Value,
                );

                writer.write(value)?;
            }
        }

        writer.finish()?;

        let metadata = Metadata::from_writer(nanoid::nanoid!().into(), writer)?;
        metadata.write_to_file()?;
        assert_eq!(ITEM_COUNT * VERSION_COUNT, metadata.item_count);
        assert_eq!(ITEM_COUNT, metadata.key_count);

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
            table,
            metadata.id,
            Some(Arc::clone(&block_cache)),
            Arc::clone(&block_index),
            None,
            None,
        );

        assert_eq!(ITEM_COUNT * VERSION_COUNT, iter.count() as u64);

        Ok(())
    }
}
