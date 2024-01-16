use super::{Choice, CompactionStrategy};
use crate::{config::PersistedConfig, levels::Levels, time::unix_timestamp};
use std::ops::Deref;

// TODO: L0 stall/halt thresholds should be configurable
// Useful in a timeseries scenario

/// FIFO-style compaction.
///
/// Limits the tree size to roughly `limit` bytes, deleting the oldest segment(s)
/// when the threshold is reached.
///
/// Will also merge segments if the amount of segments in level 0 grows too much, which
/// could cause write stalls.
///
/// Additionally, a TTL can be configured to drop old segments.
///
/// ###### Caution
///
/// Only use it for specific workloads where:
///
/// 1) You only want to store recent data (unimportant logs, ...)
/// 2) Your keyspace grows monotonically (time series)
/// 3) You only insert new data
///
/// More info here: <https://github.com/facebook/rocksdb/wiki/FIFO-compaction-style>
pub struct Strategy {
    /// Data set size limit in bytes
    limit: u64,

    /// TTL in seconds, will be disabled if 0 or None
    ttl_seconds: Option<u64>,
}

impl Strategy {
    /// Configures a new `Fifo` compaction strategy
    #[must_use]
    pub fn new(limit: u64, ttl_seconds: Option<u64>) -> Self {
        Self { limit, ttl_seconds }
    }
}

impl CompactionStrategy for Strategy {
    fn choose(&self, levels: &Levels, config: &PersistedConfig) -> Choice {
        let resolved_view = levels.resolved_view();

        let mut first_level = resolved_view
            .first()
            .expect("L0 should always exist")
            .deref()
            .clone();

        let mut segment_ids_to_delete = vec![];

        if let Some(ttl_seconds) = self.ttl_seconds {
            if ttl_seconds > 0 {
                let now = unix_timestamp().as_micros();

                for segment in &first_level {
                    let lifetime_us = now - segment.metadata.created_at;
                    let lifetime_sec = lifetime_us / 1000 / 1000;

                    eprintln!("TTL: {lifetime_sec} > {ttl_seconds}");

                    if lifetime_sec > ttl_seconds.into() {
                        segment_ids_to_delete.push(segment.metadata.id.clone());
                    }
                }
            }
        }

        let db_size = levels.size();

        if db_size > self.limit {
            let mut bytes_to_delete = db_size - self.limit;

            // Sort the level by oldest to newest
            first_level.sort_by(|a, b| a.metadata.seqnos.0.cmp(&b.metadata.seqnos.0));

            for segment in first_level {
                if bytes_to_delete == 0 {
                    break;
                }

                bytes_to_delete = bytes_to_delete.saturating_sub(segment.metadata.file_size);

                segment_ids_to_delete.push(segment.metadata.id.clone());
            }
        }

        if segment_ids_to_delete.is_empty() {
            super::maintenance::Strategy.choose(levels, config)
        } else {
            Choice::DeleteSegments(segment_ids_to_delete)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Strategy;
    use crate::{
        block_cache::BlockCache,
        compaction::{Choice, CompactionStrategy},
        config::PersistedConfig,
        descriptor_table::FileDescriptorTable,
        file::LEVELS_MANIFEST_FILE,
        levels::Levels,
        segment::{index::BlockIndex, meta::Metadata, Segment},
        time::unix_timestamp,
    };
    use std::sync::Arc;
    use test_log::test;

    #[cfg(feature = "bloom")]
    use crate::bloom::BloomFilter;

    #[allow(clippy::expect_used)]
    fn fixture_segment(id: Arc<str>, created_at: u128) -> Arc<Segment> {
        let block_cache = Arc::new(BlockCache::with_capacity_bytes(u64::MAX));

        Arc::new(Segment {
            descriptor_table: Arc::new(FileDescriptorTable::new(512, 1)),
            block_index: Arc::new(BlockIndex::new(id.clone(), block_cache.clone())),
            metadata: Metadata {
                path: ".".into(),
                version: crate::version::Version::V0,
                block_count: 0,
                block_size: 0,
                created_at,
                id,
                file_size: 1,
                compression: crate::segment::meta::CompressionType::Lz4,
                item_count: 0,
                key_count: 0,
                key_range: (vec![].into(), vec![].into()),
                tombstone_count: 0,
                uncompressed_size: 0,
                seqnos: (0, 0),
            },
            block_cache,

            #[cfg(feature = "bloom")]
            bloom_filter: BloomFilter::with_fp_rate(1, 0.1),
        })
    }

    #[test]
    fn ttl() -> crate::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let compactor = Strategy::new(u64::MAX, Some(5_000));

        let mut levels = Levels::create_new(4, tempdir.path().join(LEVELS_MANIFEST_FILE))?;

        levels.add(fixture_segment("1".into(), 1));
        levels.add(fixture_segment("2".into(), unix_timestamp().as_micros()));

        assert_eq!(
            compactor.choose(&levels, &PersistedConfig::default()),
            Choice::DeleteSegments(vec!["1".into()])
        );

        Ok(())
    }

    #[test]
    fn empty_levels() -> crate::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let compactor = Strategy::new(1, None);

        let levels = Levels::create_new(4, tempdir.path().join(LEVELS_MANIFEST_FILE))?;

        assert_eq!(
            compactor.choose(&levels, &PersistedConfig::default()),
            Choice::DoNothing
        );

        Ok(())
    }

    #[test]
    fn below_limit() -> crate::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let compactor = Strategy::new(4, None);

        let mut levels = Levels::create_new(4, tempdir.path().join(LEVELS_MANIFEST_FILE))?;

        levels.add(fixture_segment("1".into(), 1));
        assert_eq!(
            compactor.choose(&levels, &PersistedConfig::default()),
            Choice::DoNothing
        );

        levels.add(fixture_segment("2".into(), 2));
        assert_eq!(
            compactor.choose(&levels, &PersistedConfig::default()),
            Choice::DoNothing
        );

        levels.add(fixture_segment("3".into(), 3));
        assert_eq!(
            compactor.choose(&levels, &PersistedConfig::default()),
            Choice::DoNothing
        );

        levels.add(fixture_segment("4".into(), 4));
        assert_eq!(
            compactor.choose(&levels, &PersistedConfig::default()),
            Choice::DoNothing
        );

        Ok(())
    }

    #[test]
    fn more_than_limit() -> crate::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let compactor = Strategy::new(2, None);

        let mut levels = Levels::create_new(4, tempdir.path().join(LEVELS_MANIFEST_FILE))?;
        levels.add(fixture_segment("1".into(), 1));
        levels.add(fixture_segment("2".into(), 2));
        levels.add(fixture_segment("3".into(), 3));
        levels.add(fixture_segment("4".into(), 4));

        assert_eq!(
            compactor.choose(&levels, &PersistedConfig::default()),
            Choice::DeleteSegments(vec!["1".into(), "2".into()])
        );

        Ok(())
    }
}
