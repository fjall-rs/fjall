use super::IndexEntry;
use crate::{disk_block::DiskBlock, serde::Serializable, value::UserKey};
use lz4_flex::compress_prepend_size;
use std::{
    fs::File,
    io::{BufWriter, Write},
    path::{Path, PathBuf},
};

pub struct Writer {
    path: PathBuf,
    file_pos: u64,
    block_writer: BufWriter<File>,
    index_writer: BufWriter<File>,
    block_size: u32,
    block_counter: u32,
    block_chunk: DiskBlock<IndexEntry>,
    index_chunk: DiskBlock<IndexEntry>,
}

impl Writer {
    pub fn new<P: AsRef<Path>>(path: P, block_size: u32) -> crate::Result<Self> {
        let block_writer = File::create(path.as_ref().join("index_blocks"))?;
        let block_writer = BufWriter::with_capacity(u16::MAX.into(), block_writer);

        let index_writer = File::create(path.as_ref().join("index"))?;
        let index_writer = BufWriter::new(index_writer);

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
            index_writer,
            block_counter: 0,
            block_size,
            block_chunk,
            index_chunk,
        })
    }

    fn write_block(&mut self) -> crate::Result<()> {
        // Serialize block
        let mut bytes = Vec::with_capacity(u16::MAX.into());
        self.block_chunk.crc = DiskBlock::<IndexEntry>::create_crc(&self.block_chunk.items)?;
        self.block_chunk
            .serialize(&mut bytes)
            .expect("should serialize block");

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

        self.index_chunk.items.push(IndexEntry {
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
        start_key: UserKey,
        offset: u64,
        size: u32,
    ) -> crate::Result<()> {
        let reference = IndexEntry {
            offset,
            size,
            start_key,
        };
        self.block_chunk.items.push(reference);

        self.block_counter += std::mem::size_of::<IndexEntry>() as u32;

        if self.block_counter >= self.block_size {
            self.write_block()?;
        }

        Ok(())
    }

    fn write_meta_index(&mut self) -> crate::Result<()> {
        // Serialize block
        let mut bytes = Vec::with_capacity(u16::MAX.into());
        self.index_chunk.crc = DiskBlock::<IndexEntry>::create_crc(&self.index_chunk.items)?;
        self.index_chunk
            .serialize(&mut bytes)
            .expect("should serialize index block");

        // Compress using LZ4
        let bytes = compress_prepend_size(&bytes);

        // Write to file
        self.index_writer.write_all(&bytes)?;

        log::debug!(
            "Written meta index to {}, with {} pointers ({} bytes)",
            self.path.join("index").display(),
            self.index_chunk.items.len(),
            bytes.len(),
        );

        Ok(())
    }

    fn flush_writers(&mut self) -> crate::Result<()> {
        // fsync data blocks
        self.block_writer.flush()?;
        self.block_writer.get_mut().sync_all()?;

        // fsync index blocks
        self.index_writer.flush()?;
        self.index_writer.get_mut().sync_all()?;

        Ok(())
    }

    pub fn finish(&mut self) -> crate::Result<()> {
        if self.block_counter > 0 {
            self.write_block()?;
        }

        self.write_meta_index()?;
        self.flush_writers()?;

        // fsync folder
        let file = std::fs::File::open(&self.path)?;
        file.sync_all()?;

        Ok(())
    }
}
