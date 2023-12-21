use super::{CompactionStrategy, Input as CompactionPayload};
use crate::{
    compaction::Choice,
    descriptor_table::FileDescriptorTable,
    file::{BLOCKS_FILE, SEGMENTS_FOLDER},
    levels::Levels,
    memtable::MemTable,
    merge::MergeIterator,
    segment::{index::BlockIndex, writer::MultiWriter, Segment},
    snapshot::SnapshotCounter, /* Tree, */
    stop_signal::StopSignal,
    Config,
};
use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock, RwLockWriteGuard},
    time::Instant,
};

/// Compaction options
pub struct Options {
    /// Configuration of tree.
    pub config: Config,

    /// Levels manifest.
    pub levels: Arc<RwLock<Levels>>,

    /// Immutable memtables (required for temporarily locking).
    pub immutable_memtables: Arc<RwLock<BTreeMap<Arc<str>, Arc<MemTable>>>>,

    /// Snapshot counter (required for checking if there are open snapshots).
    pub open_snapshots: SnapshotCounter,

    /// Compaction strategy.
    ///
    /// The one inside `config` is NOT used.
    pub strategy: Box<dyn CompactionStrategy>,

    /// Stop signal
    pub stop_signal: StopSignal,
}

/// Runs compaction task.
///
/// This will block until the compactor is fully finished.
pub fn do_compaction(opts: &Options) -> crate::Result<()> {
    log::debug!("compactor: acquiring levels manifest lock");
    let mut levels = opts.levels.write().expect("lock is poisoned");

    log::trace!("compactor: consulting compaction strategy");
    let choice = opts.strategy.choose(&levels, &opts.config);

    match choice {
        Choice::DoCompact(payload) => {
            merge_segments(levels, opts, &payload)?;
        }
        Choice::DeleteSegments(payload) => {
            drop_segments(levels, opts, &payload)?;
        }
        Choice::DoNothing => {
            log::trace!("Compactor chose to do nothing");
        }
    }

    Ok(())
}

fn merge_segments(
    mut levels: RwLockWriteGuard<'_, Levels>,
    opts: &Options,
    payload: &CompactionPayload,
) -> crate::Result<()> {
    if opts.stop_signal.is_stopped() {
        log::debug!("compactor: stopping before compaction because of stop signal");
    }

    log::debug!(
        "compactor: Chosen {} segments to compact into a single new segment at level {}",
        payload.segment_ids.len(),
        payload.dest_level
    );

    let merge_iter = {
        let to_merge: Vec<Arc<Segment>> = {
            let segments = levels.get_all_segments();
            payload
                .segment_ids
                .iter()
                .filter_map(|x| segments.get(x))
                .cloned()
                .collect()
        };

        // NOTE: When there are open snapshots
        // we don't want to GC old versions of items
        // otherwise snapshots will lose data
        let no_snapshots_open = !opts.open_snapshots.has_open_snapshots();

        MergeIterator::from_segments(&to_merge).evict_old_versions(no_snapshots_open)
    };

    let last_level = levels.last_level_index();

    levels.hide_segments(&payload.segment_ids);
    drop(levels);

    // NOTE: Only evict tombstones when reaching the last level,
    // That way we don't resurrect data beneath the tombstone
    let should_evict_tombstones = payload.dest_level == last_level;

    let start = Instant::now();

    let mut segment_writer = MultiWriter::new(
        payload.target_size,
        crate::segment::writer::Options {
            block_size: opts.config.block_size,
            evict_tombstones: should_evict_tombstones,
            path: opts.config.path.join(SEGMENTS_FOLDER),
        },
    )?;

    for (idx, item) in merge_iter.enumerate() {
        segment_writer.write(item?)?;

        if idx % 100_000 == 0 && opts.stop_signal.is_stopped() {
            log::debug!("compactor: stopping amidst compaction because of stop signal");
            return Ok(());
        }
    }

    let created_segments = segment_writer.finish()?;

    log::debug!(
        "Compacted in {}ms ({} segments created)",
        start.elapsed().as_millis(),
        created_segments.len()
    );

    for metadata in &created_segments {
        metadata.write_to_file()?;
    }

    let created_segments = created_segments
        .into_iter()
        .map(|metadata| -> crate::Result<Segment> {
            let segment_id = metadata.id.clone();
            let path = metadata.path.clone();

            let descriptor_table =
                Arc::new(FileDescriptorTable::new(metadata.path.join(BLOCKS_FILE))?);

            Ok(Segment {
                descriptor_table: descriptor_table.clone(),
                metadata,
                block_cache: opts.config.block_cache.clone(),
                block_index: BlockIndex::from_file(
                    segment_id,
                    descriptor_table,
                    path,
                    opts.config.block_cache.clone(),
                )?
                .into(),
            })
        })
        .collect::<crate::Result<Vec<_>>>()?;

    log::debug!("compactor: acquiring levels manifest write lock");
    let mut levels = opts.levels.write().expect("lock is poisoned");

    // NOTE: Write lock memtable, otherwise segments may get deleted while a range read is happening
    log::debug!("compactor: acquiring immu memtables write lock");
    let memtable = opts.immutable_memtables.write().expect("lock is poisoned");

    let segments_base_folder = opts.config.path.join(SEGMENTS_FOLDER);

    for segment in created_segments {
        log::trace!("Persisting segment {}", segment.metadata.id);
        levels.insert_into_level(payload.dest_level, segment.into());
    }

    for key in &payload.segment_ids {
        log::trace!("Removing segment {}", key);
        levels.remove(key);
    }

    // NOTE: This is really important
    // Write the segment with the removed segments first
    // Otherwise the folder is deleted, but the segment is still referenced!
    levels.write_to_disk()?;

    for key in &payload.segment_ids {
        let segment_folder = segments_base_folder.join(&**key);

        log::trace!("rm -rf segment folder at {}", segment_folder.display());
        std::fs::remove_dir_all(segment_folder)?;
    }

    levels.show_segments(&payload.segment_ids);

    drop(memtable);
    drop(levels);

    log::debug!("compactor: done");

    Ok(())
}

fn drop_segments(
    mut levels: RwLockWriteGuard<'_, Levels>,
    opts: &Options,
    segment_ids: &[Arc<str>],
) -> crate::Result<()> {
    log::debug!("compactor: Chosen {} segments to drop", segment_ids.len(),);

    // NOTE: Write lock memtable, otherwise segments may get deleted while a range read is happening
    log::debug!("compaction: acquiring immu memtables write lock");
    let memtable_lock = opts.immutable_memtables.write().expect("lock is poisoned");

    for key in segment_ids {
        log::trace!("Removing segment {}", key);
        levels.remove(key);
    }

    // NOTE: This is really important
    // Write the segment with the removed segments first
    // Otherwise the folder is deleted, but the segment is still referenced!
    levels.write_to_disk()?;

    drop(memtable_lock);
    drop(levels);

    for key in segment_ids {
        log::trace!("rm -rf segment folder {}", key);
        std::fs::remove_dir_all(opts.config.path.join(SEGMENTS_FOLDER).join(&**key))?;
    }

    log::trace!("Dropped {} segments", segment_ids.len());

    Ok(())
}
