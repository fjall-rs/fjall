use std::{fs::File, io::Write, path::Path};

#[doc(hidden)]
pub const LSM_MARKER: &str = ".lsm";
pub const SEGMENTS_FOLDER: &str = "segments";
pub const LEVELS_MANIFEST_FILE: &str = "levels.json";
pub const CONFIG_FILE: &str = "config.json";

pub const BLOCKS_FILE: &str = "blocks";
pub const INDEX_BLOCKS_FILE: &str = "index_blocks";
pub const TOP_LEVEL_INDEX_FILE: &str = "index";
pub const SEGMENT_METADATA_FILE: &str = "meta.json";

#[cfg(feature = "bloom")]
pub const BLOOM_FILTER_FILE: &str = "bloom";

/// Atomically rewrites a file
pub fn rewrite_atomic<P: AsRef<Path>>(path: P, content: &[u8]) -> std::io::Result<()> {
    let path = path.as_ref();
    let folder = path.parent().expect("should have parent folder");

    let mut temp_file = tempfile::NamedTempFile::new_in(folder)?;
    temp_file.write_all(content)?;
    temp_file.persist(path)?;

    #[cfg(not(target_os = "windows"))]
    {
        // TODO: Not sure if the fsync is really required, but just for the sake of it...
        // TODO: also not sure why it fails on Windows...
        let file = File::open(path)?;
        file.sync_all()?;
    }

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
        {
            let mut file = File::create(&path)?;
            write!(file, "asdasdasdasdasd")?;
        }

        rewrite_atomic(&path, b"newcontent")?;

        let content = std::fs::read_to_string(&path)?;
        assert_eq!("newcontent", content);

        Ok(())
    }
}
