use crate::sharded::Sharded;
use std::{
    fs::File,
    path::Path,
    sync::{RwLock, RwLockWriteGuard},
};

pub struct FileDescriptorTable {
    // TODO: bufreader or file...?
    files: Sharded<File>,
}

const SHARD_COUNT: usize = 4;

impl FileDescriptorTable {
    pub fn new<P: AsRef<Path>>(path: P) -> crate::Result<Self> {
        let shards = (0..SHARD_COUNT)
            .map(|_| {
                let file = File::open(&path)?;
                let shard = RwLock::new(file);
                Ok(shard)
            })
            .collect::<crate::Result<Vec<_>>>()?;

        Ok(Self {
            files: Sharded::new(shards),
        })
    }

    //  TODO: benchmark mutex
    pub fn access(&self) -> RwLockWriteGuard<'_, File> {
        self.files.write_one()
    }
}
