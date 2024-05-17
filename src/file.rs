use std::path::Path;

pub const JOURNALS_FOLDER: &str = "journals";
pub const SEGMENTS_FOLDER: &str = "segments";
pub const PARTITIONS_FOLDER: &str = "partitions";
pub const FJALL_MARKER: &str = "version";
pub const PARTITION_DELETED_MARKER: &str = ".deleted";

pub const FLUSH_PARTITIONS_LIST: &str = ".partitions";
pub const FLUSH_MARKER: &str = ".flush";

#[cfg(not(target_os = "windows"))]
pub fn fsync_directory<P: AsRef<Path>>(path: P) -> std::io::Result<()> {
    let file = std::fs::File::open(path)?;
    debug_assert!(file.metadata()?.is_dir());
    file.sync_all()
}

#[cfg(target_os = "windows")]
pub fn fsync_directory<P: AsRef<Path>>(_path: P) -> std::io::Result<()> {
    // Cannot fsync directory on Windows
    Ok(())
}
