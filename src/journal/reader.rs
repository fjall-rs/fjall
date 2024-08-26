// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{
    get_shard_path,
    shard::{
        batch_reader::{Batch, JournalShardBatchReader},
        reader::JournalShardReader,
    },
    SHARD_COUNT,
};
use std::{iter::Peekable, path::Path};

#[allow(clippy::module_name_repetitions)]
pub struct JournalReader {
    readers: Vec<Peekable<JournalShardBatchReader>>,
}

impl JournalReader {
    pub fn new<P: AsRef<Path>>(path: P) -> crate::Result<Self> {
        let path = path.as_ref();

        let mut readers = Vec::with_capacity(SHARD_COUNT.into());

        for idx in 0..SHARD_COUNT {
            let shard_path = get_shard_path(path, idx);

            if shard_path.exists() {
                readers.push(
                    JournalShardBatchReader::new(JournalShardReader::new(shard_path)?).peekable(),
                );
            }
        }

        Ok(Self { readers })
    }
}

impl Iterator for JournalReader {
    type Item = crate::Result<Batch>;

    fn next(&mut self) -> Option<Self::Item> {
        let peeked = self
            .readers
            .iter_mut()
            .enumerate()
            .map(|(idx, shard)| (idx, shard.peek()));

        let mut min = None;

        for (idx, batch) in peeked {
            if let Some(batch) = batch {
                match batch {
                    Ok(batch) => match min {
                        Some((_, min_seqno)) => {
                            if batch.seqno < min_seqno {
                                min = Some((idx, batch.seqno));
                            }
                        }
                        None => {
                            min = Some((idx, batch.seqno));
                        }
                    },
                    Err(_) => {
                        let reader = self.readers.get_mut(idx).expect("should exist");

                        let err = reader
                            .next()
                            .expect("should exist")
                            .expect_err("should be error");

                        return Some(Err(err));
                    }
                }
            }
        }

        if let Some((idx, _)) = min {
            let item = self
                .readers
                .get_mut(idx)
                .expect("should exist")
                .next()
                .expect("should exist");

            Some(item)
        } else {
            None
        }
    }
}
