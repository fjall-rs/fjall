use super::{Choice, CompactionStrategy};
use crate::levels::Levels;
use std::sync::Arc;

/// FIFO-style compaction
///
/// Limits the tree size to roughly `limit` bytes, deleting the oldest segment(s)
/// when the threshold is reached
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
    limit: u64,
}

impl Strategy {
    /// Configures a new `Fifo` compaction strategy
    ///
    /// # Examples
    ///
    /// ```
    /// use lsm_tree::{Config, compaction::Fifo};
    ///
    /// let one_gb = 1 * 1_024 * 1_024 * 1_024;
    ///
    /// let config = Config::default().compaction_strategy(Fifo::new(one_gb));
    /// ```
    #[must_use]
    pub fn new(limit: u64) -> Arc<Self> {
        Arc::new(Self { limit })
    }
}

impl CompactionStrategy for Strategy {
    fn choose(&self, levels: &Levels) -> Choice {
        let resolved_view = levels.resolved_view();

        let mut first_level = resolved_view[0].clone();

        let db_size = levels
            .get_all_segments()
            .values()
            .map(|x| x.metadata.file_size)
            .sum::<u64>();

        if db_size > self.limit {
            let mut bytes_to_delete = db_size - self.limit;

            // Sort the level by creation date
            first_level.sort_by(|a, b| a.metadata.created_at.cmp(&b.metadata.created_at));

            let mut ids = vec![];

            for segment in first_level {
                if bytes_to_delete == 0 {
                    break;
                }

                bytes_to_delete = bytes_to_delete.saturating_sub(segment.metadata.file_size);

                ids.push(segment.metadata.id.clone());
            }

            Choice::DeleteSegments(ids)
        } else {
            Choice::DoNothing
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Strategy;
    use crate::{
        block_cache::BlockCache,
        compaction::{Choice, CompactionStrategy},
        descriptor_table::FileDescriptorTable,
        file::LEVELS_MANIFEST_FILE,
        levels::Levels,
        segment::{index::BlockIndex, meta::Metadata, Segment},
    };
    use std::sync::Arc;
    use test_log::test;

    fn fixture_segment(id: String, created_at: u128) -> Arc<Segment> {
        let block_cache = Arc::new(BlockCache::new(0));

        Arc::new(Segment {
            // NOTE: It's just a test
            #[allow(clippy::expect_used)]
            descriptor_table: Arc::new(
                FileDescriptorTable::new("Cargo.toml").expect("should open"),
            ),
            block_index: Arc::new(
                BlockIndex::new(id.clone(), block_cache.clone()).expect("should create index"),
            ),
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
                key_range: (vec![].into(), vec![].into()),
                tombstone_count: 0,
                uncompressed_size: 0,
                seqnos: (0, 0),
            },
            block_cache,
        })
    }

    #[test]
    fn empty_levels() -> crate::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let compactor = Strategy::new(1);

        let levels = Levels::create_new(4, tempdir.path().join(LEVELS_MANIFEST_FILE))?;

        assert_eq!(compactor.choose(&levels), Choice::DoNothing);

        Ok(())
    }

    #[test]
    fn below_limit() -> crate::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let compactor = Strategy::new(4);

        let mut levels = Levels::create_new(4, tempdir.path().join(LEVELS_MANIFEST_FILE))?;

        levels.add(fixture_segment("1".into(), 1));
        assert_eq!(compactor.choose(&levels), Choice::DoNothing);

        levels.add(fixture_segment("2".into(), 2));
        assert_eq!(compactor.choose(&levels), Choice::DoNothing);

        levels.add(fixture_segment("3".into(), 3));
        assert_eq!(compactor.choose(&levels), Choice::DoNothing);

        levels.add(fixture_segment("4".into(), 4));
        assert_eq!(compactor.choose(&levels), Choice::DoNothing);

        Ok(())
    }

    #[test]
    fn more_than_limit() -> crate::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let compactor = Strategy::new(2);

        let mut levels = Levels::create_new(4, tempdir.path().join(LEVELS_MANIFEST_FILE))?;
        levels.add(fixture_segment("1".into(), 1));
        levels.add(fixture_segment("2".into(), 2));
        levels.add(fixture_segment("3".into(), 3));
        levels.add(fixture_segment("4".into(), 4));

        assert_eq!(
            compactor.choose(&levels),
            Choice::DeleteSegments(vec!["1".into(), "2".into()])
        );

        Ok(())
    }
}
