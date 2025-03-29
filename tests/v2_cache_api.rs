// TODO: remove in V3

#[allow(deprecated)]
use fjall::{BlobCache, BlockCache};
use std::sync::Arc;

#[test_log::test]
#[allow(deprecated)]
fn v2_cache_api() -> fjall::Result<()> {
    use fjall::Config;

    let folder = tempfile::tempdir()?;

    let keyspace = Config::new(&folder)
        .blob_cache(Arc::new(BlobCache::with_capacity_bytes(64_000)))
        .block_cache(Arc::new(BlockCache::with_capacity_bytes(64_000)))
        .open()?;

    assert_eq!(keyspace.cache_capacity(), 128_000);

    Ok(())
}
