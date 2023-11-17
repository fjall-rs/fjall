use lz4_flex::compress_prepend_size;

use super::BlockIndexEntry;
use crate::{disk_block::DiskBlock, serde::Serializable};
use std::{
    fs::File,
    io::{BufWriter, Write},
    path::{Path, PathBuf},
};

pub struct Writer {
    path: PathBuf,
    file_pos: u64,
    block_writer: BufWriter<File>,
    block_size: u32,
    block_counter: u32,
    block_chunk: DiskBlock<BlockIndexEntry>,
    index_chunk: DiskBlock<BlockIndexEntry>,
}

impl Writer {
    pub fn new<P: AsRef<Path>>(path: P, block_size: u32) -> std::io::Result<Self> {
        let block_writer = File::create(path.as_ref().join("index_blocks"))?;
        let block_writer = BufWriter::with_capacity(128_000, block_writer);

        let block_chunk = DiskBlock {
            items: vec![],
            crc: 0,
        };

        let index_chunk = DiskBlock {
            items: vec![],
            crc: 0,
        };

        Ok(Self {
            path: path.as_ref().into(),
            file_pos: 0,
            block_writer,
            block_counter: 0,
            block_size,
            block_chunk,
            index_chunk,
        })
    }

    fn write_block(&mut self) -> std::io::Result<()> {
        // Serialize block
        let mut bytes = Vec::with_capacity(u16::MAX.into());
        self.block_chunk.crc = DiskBlock::<BlockIndexEntry>::create_crc(&self.block_chunk.items);
        self.block_chunk.serialize(&mut bytes).unwrap();

        // Compress using LZ4
        let bytes = compress_prepend_size(&bytes);

        // Write to file
        self.block_writer.write_all(&bytes)?;

        // Expect is fine, because the chunk is not empty
        let first = self
            .block_chunk
            .items
            .first()
            .expect("Chunk should not be empty");

        let bytes_written = bytes.len();

        self.index_chunk.items.push(BlockIndexEntry {
            start_key: first.start_key.clone(),
            offset: self.file_pos,
            size: bytes_written as u32,
        });

        log::trace!(
            "Written index block @ {} ({bytes_written} bytes)",
            self.file_pos,
        );

        self.block_counter = 0;
        self.block_chunk.items.clear();
        self.file_pos += bytes_written as u64;

        Ok(())
    }

    pub fn register_block(
        &mut self,
        start_key: Vec<u8>,
        offset: u64,
        size: u32,
    ) -> std::io::Result<()> {
        let reference = BlockIndexEntry {
            offset,
            size,
            start_key,
        };
        self.block_chunk.items.push(reference);

        self.block_counter += std::mem::size_of::<BlockIndexEntry>() as u32;

        if self.block_counter >= self.block_size {
            self.write_block()?;
        }

        Ok(())
    }

    pub fn finalize(&mut self) -> std::io::Result<()> {
        if self.block_counter > 0 {
            self.write_block()?;
        }

        let index_writer = File::create(self.path.join("index"))?;
        let mut index_writer = BufWriter::with_capacity(128_000, index_writer);

        // Serialize block
        let mut bytes = Vec::with_capacity(u16::MAX.into());
        self.index_chunk.crc = DiskBlock::<BlockIndexEntry>::create_crc(&self.index_chunk.items);
        self.index_chunk.serialize(&mut bytes).unwrap();

        // Compress using LZ4
        let bytes = compress_prepend_size(&bytes);

        // Write to file
        index_writer.write_all(&bytes)?;

        // Flush to disk
        self.block_writer.flush()?;
        self.block_writer.get_mut().sync_all()?;

        index_writer.flush()?;
        index_writer.get_mut().sync_all()?;

        log::debug!(
            "Written meta index, with {} pointers",
            self.index_chunk.items.len()
        );

        Ok(())
    }
}
