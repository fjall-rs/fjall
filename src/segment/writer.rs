use super::{block::ValueBlock, meta::Metadata};
use crate::{
    id::generate_table_id, segment::index::writer::Writer as IndexWriter, serde::Serializable,
    value::SeqNo, Value,
};
use lz4_flex::compress_prepend_size;
use std::{
    fs::File,
    io::{BufWriter, Write},
    path::PathBuf,
};

/// Like `Writer` but will rotate to a new segment, once a segment grows larger than `target_size`
///
/// This results in a sorted "run" of segments
pub struct MultiWriter {
    /// Target size of segments in bytes
    ///
    /// If a segment reaches the target size, a new one is started,
    /// resulting in a sorted "run" of segments
    pub target_size: u64,

    pub opts: Options,
    created_items: Vec<Metadata>,

    pub current_segment_id: String,
    pub writer: Writer,
}

impl MultiWriter {
    /// Sets up a new `MultiWriter` at the given segments folder
    pub fn new(target_size: u64, opts: Options) -> crate::Result<Self> {
        let segment_id = generate_table_id();

        let writer = Writer::new(Options {
            path: opts.path.join(&segment_id),
            evict_tombstones: opts.evict_tombstones,
            block_size: opts.block_size,
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

        let new_segment_id = generate_table_id();

        let new_writer = Writer::new(Options {
            path: self.opts.path.join(&new_segment_id),
            evict_tombstones: self.opts.evict_tombstones,
            block_size: self.opts.block_size,
        })?;

        let old_writer = std::mem::replace(&mut self.writer, new_writer);
        let old_segment_id = std::mem::replace(&mut self.current_segment_id, new_segment_id);

        self.created_items
            .push(Metadata::from_writer(old_segment_id, old_writer));

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
        self.rotate()?;
        // TODO: this causes empty segments...

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
    pub uncompressed_size: u64,

    pub first_key: Option<Vec<u8>>,
    pub last_key: Option<Vec<u8>>,
    pub tombstone_count: usize,
    pub chunk_size: usize,

    pub lowest_seqno: SeqNo,
    pub highest_seqno: SeqNo,
}

pub struct Options {
    pub path: PathBuf,
    pub evict_tombstones: bool,
    pub block_size: u32,
}

impl Writer {
    /// Sets up a new `MultiWriter` at the given segments folder
    pub fn new(opts: Options) -> std::io::Result<Self> {
        std::fs::create_dir_all(&opts.path)?;

        let block_writer = File::create(opts.path.join("blocks"))?;
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
        })
    }

    /// Writes a compressed block to disk
    ///
    /// This is triggered when a `Writer::write` causes the buffer to grow to the configured `block_size`
    fn write_block(&mut self) -> std::io::Result<()> {
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
        self.chunk.crc = ValueBlock::create_crc(&self.chunk.items);
        self.chunk.serialize(&mut bytes).unwrap();

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

        // TODO:  Add to bloom filter

        // Adjust metadata
        log::trace!(
            "Written data block @ {} ({} bytes, uncompressed: {} bytes)",
            self.file_pos,
            bytes_written,
            uncompressed_chunk_size
        );

        self.file_pos += u64::from(bytes_written);
        self.item_count += self.chunk.items.len();
        self.block_count += 1;
        self.chunk.items.clear();

        Ok(())
    }

    /// Writes an item
    pub fn write(&mut self, item: Value) -> std::io::Result<()> {
        if item.is_tombstone {
            if self.opts.evict_tombstones {
                return Ok(());
            }

            self.tombstone_count += 1;
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
    pub fn finish(&mut self) -> std::io::Result<()> {
        if !self.chunk.items.is_empty() {
            self.write_block()?;
        }

        // TODO: bloom etc

        self.index_writer.finish()?;

        self.block_writer.flush()?;
        self.block_writer.get_mut().sync_all()?;

        // fsync folder
        let folder = std::fs::File::open(&self.opts.path)?;
        folder.sync_all()?;

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
    use crate::{
        block_cache::BlockCache,
        segment::{index::MetaIndex, meta::Metadata, reader::Reader},
        Value,
    };
    use std::sync::Arc;
    use test_log::test;

    #[test]
    fn test_write_and_read() {
        const ITEM_COUNT: u64 = 100_000;

        let folder = tempfile::tempdir().unwrap().into_path();

        let mut writer = Writer::new(Options {
            path: folder.clone(),
            evict_tombstones: false,
            block_size: 4096,
        })
        .unwrap();

        let items =
            (0u64..ITEM_COUNT).map(|i| Value::new(i.to_be_bytes(), nanoid::nanoid!(), false, 0));

        for item in items {
            writer.write(item).unwrap();
        }

        writer.finish().unwrap();

        let metadata = Metadata::from_writer(nanoid::nanoid!(), writer);
        metadata.write_to_file().unwrap();
        assert_eq!(ITEM_COUNT, metadata.item_count);

        let block_cache = Arc::new(BlockCache::new(usize::MAX));
        let meta_index = Arc::new(
            MetaIndex::from_file(metadata.id.clone(), &folder, Arc::clone(&block_cache)).unwrap(),
        );
        let iter = Reader::new(
            folder.join("blocks"),
            metadata.id,
            Arc::clone(&block_cache),
            Arc::clone(&meta_index),
            None,
            None,
        )
        .unwrap();

        assert_eq!(ITEM_COUNT, iter.count() as u64);

        /*  log::info!("Getting every item");

        let mut iter =
            Reader::new(folder.join("blocks"), Arc::clone(&meta_index), None, None).unwrap();

        for key in (0u64..ITEM_COUNT).map(u64::to_be_bytes) {
            let item = iter.next().unwrap().expect("item should exist");
            assert_eq!(key, &*item.key);
        }

        log::info!("Getting every item in reverse");

        let mut iter =
            Reader::new(folder.join("blocks"), Arc::clone(&meta_index), None, None).unwrap();

        for key in (0u64..ITEM_COUNT).rev().map(u64::to_be_bytes) {
            let item = iter.next_back().unwrap().expect("item should exist");
            assert_eq!(key, &*item.key);
        }

        log::info!("Getting every item in range");

        let mut iter = Range::new(
            folder.join("blocks"),
            Arc::clone(&meta_index),
            (
                Included(0u64.to_be_bytes().into()),
                Excluded(100u64.to_be_bytes().into()),
            ),
        )
        .unwrap();

        for key in (0u64..100).map(u64::to_be_bytes) {
            let item = iter.next().unwrap().expect("item should exist");
            assert_eq!(key, &*item.key);
        }

        log::info!("Getting every item in range in reverse");

        let mut iter = Range::new(
            folder.join("blocks"),
            Arc::clone(&meta_index),
            (
                Included(0u64.to_be_bytes().into()),
                Excluded(100u64.to_be_bytes().into()),
            ),
        )
        .unwrap();

        for key in (0u64..100).rev().map(u64::to_be_bytes) {
            let item = iter.next_back().unwrap().expect("item should exist");
            assert_eq!(key, &*item.key);
        } */

        //   Reader::new(folder.join("blocks"), Arc::clone(&meta_index), None, None).unwrap();

        /* for thread_count in [1, 1, 2, 4, 8, 16] {
            let start = std::time::Instant::now();

            let threads = (0..thread_count)
                .map(|thread_no| {
                    let meta_index = meta_index.clone();

                    std::thread::spawn(move || {
                        let item_count = ITEM_COUNT / thread_count;
                        let start = thread_no * item_count;
                        let range = start..(start + item_count);

                        for key in range.map(u64::to_be_bytes) {
                            let item = meta_index.get_latest(&key);

                            match item {
                                Some(item) => {
                                    assert_eq!(key, &*item.key);
                                }
                                None => {
                                    panic!("item should exist: {}", u64::from_be_bytes(key))
                                }
                            }
                        }
                    })
                })
                .collect::<Vec<_>>();

            for thread in threads {
                thread.join().unwrap();
            }

            let elapsed = start.elapsed();
            let nanos = elapsed.as_nanos();
            let nanos_per_item = nanos / u128::from(ITEM_COUNT);
            let reads_per_second = (std::time::Duration::from_secs(1)).as_nanos() / nanos_per_item;

            eprintln!(
                "done in {:?}s, {}ns per item - {} RPS",
                elapsed.as_secs_f64(),
                nanos_per_item,
                reads_per_second
            );
        } */
    }
}
