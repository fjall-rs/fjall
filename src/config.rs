use std::path::{Path, PathBuf};

use crate::Tree;

/// Tree configuration
pub struct Config {
    /// Folder path
    ///
    /// Defaults to `./.lsm.data`
    pub(crate) path: PathBuf,

    /// Block size of data and index blocks
    ///
    /// Defaults to 4 KiB (4096 bytes)
    pub(crate) block_size: u32,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            path: ".lsm.data".into(),
            block_size: 4_096,
        }
    }
}

impl Config {
    /// Initializes a new config
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        Self {
            path: path.as_ref().into(),
            block_size: 4_096,
        }
    }

    /// Sets the block size
    ///
    /// # Panics
    ///
    /// Panics if the block size is smaller than 1 KiB (1024 bytes)
    #[must_use]
    pub fn block_size(mut self, block_size: u32) -> Self {
        assert!(block_size >= 1024);

        self.block_size = block_size;
        self
    }

    /// Opens a tree using the config
    ///
    /// # Errors
    ///
    /// - Will return `Err` if an IO error occurs
    pub fn open(self) -> crate::Result<Tree> {
        Tree::open(self)
    }
}
