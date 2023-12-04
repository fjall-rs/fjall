use std::{
    fs::File,
    io::{BufWriter, Write},
};

use super::Levels;
use crate::time::unix_timestamp;
use serde_json::json;

const SEGMENT_HISTORY_PATH: &str = "./segment_history.jsonl";

pub struct Writer {
    file: BufWriter<File>,
}

impl Writer {
    pub fn new() -> crate::Result<Self> {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(SEGMENT_HISTORY_PATH)?;
        let file = BufWriter::new(file);

        Ok(Self { file })
    }

    pub fn write(&mut self, line: &str) -> crate::Result<()> {
        writeln!(&mut self.file, "{line}")?;
        Ok(())
    }
}
