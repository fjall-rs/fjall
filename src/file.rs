use std::{
    fs::{rename, File},
    io::Write,
    path::Path,
};

pub const LSM_MARKER: &str = ".lsm";
pub const LEVELS_MANIFEST_FILE: &str = "levels.json";
pub const JOURNALS_FOLDER: &str = "journals";

pub const SEGMENTS_FOLDER: &str = "segments";
pub const BLOCKS_FILE: &str = "blocks";
pub const INDEX_BLOCKS_FILE: &str = "index_blocks";
pub const TOP_LEVEL_INDEX_FILE: &str = "index";
pub const SEGMENT_METADATA_FILE: &str = "meta.json";

/// Atomically rewrites a file
pub fn rewrite_atomic<P: AsRef<Path>>(path: P, content: &[u8]) -> std::io::Result<()> {
    let path = path.as_ref();

    let tmp: String = format!(
        "~{}",
        path.file_name()
            .and_then(std::ffi::OsStr::to_str)
            .expect("should be valid filename")
    );

    let temp_path = path
        .parent()
        .expect("level manifest should have parent folder")
        .join(tmp);

    let mut temp_file = File::create(&temp_path)?;
    temp_file.write_all(content)?;

    // TODO: this may not work on Windows
    // Use https://docs.rs/tempfile/latest/tempfile/struct.NamedTempFile.html#method.persist
    rename(&temp_path, path)?;

    // fsync file
    let file = File::open(path)?;
    file.sync_all()?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use test_log::test;

    #[test]
    fn test_atomic_rewrite() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;

        let path = dir.path().join("test.txt");
        let mut file = File::create(&path)?;
        write!(file, "asdasdasdasdasd")?;

        rewrite_atomic(&path, b"newcontent")?;

        let content = std::fs::read_to_string(&path)?;
        assert_eq!("newcontent", content);

        Ok(())
    }
}
