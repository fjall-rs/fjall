use super::CompactionStrategy;
use crate::{
    block_cache::BlockCache,
    compaction::Choice,
    descriptor_table::FileDescriptorTable,
    file::{BLOCKS_FILE, SEGMENTS_FOLDER},
    levels::Levels,
    memtable::MemTable,
    merge::MergeIterator,
    segment::{index::BlockIndex, writer::MultiWriter, Segment},
    stop_signal::StopSignal,
    Config, Tree,
};
use std::{
    collections::BTreeMap,
    sync::{atomic::AtomicU32, Arc, RwLock},
    time::Instant,
};

pub fn do_compaction(
    config: &Config,
    levels: &Arc<RwLock<Levels>>,
    stop_signal: &StopSignal,
    immutable_memtables: &Arc<RwLock<BTreeMap<String, Arc<MemTable>>>>,
    open_snapshots: &Arc<AtomicU32>,
    block_cache: &Arc<BlockCache>,
    payload: &crate::compaction::Input,
) -> crate::Result<()> {
    if stop_signal.is_stopped() {
        log::debug!("Got stop signal: compaction thread is stopping");
        return Ok(());
    }

    let start = Instant::now();

    log::debug!(
        "Chosen {} segments to compact into a single new segment at level {}",
        payload.segment_ids.len(),
        payload.dest_level
    );

    let mut segments_lock = levels.write().expect("lock is poisoned");

    let merge_iter = {
        let to_merge: Vec<Arc<Segment>> = {
            let segments = segments_lock.get_segments();
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
        let snapshot_count = open_snapshots.load(std::sync::atomic::Ordering::Acquire);
        let no_snapshots_open = snapshot_count == 0;

        MergeIterator::from_segments(&to_merge).evict_old_versions(no_snapshots_open)
    };

    segments_lock.hide_segments(&payload.segment_ids);
    drop(segments_lock);
    log::trace!("Freed segment lock");

    // NOTE: Only evict tombstones when reaching the last level,
    // That way we don't resurrect data beneath the tombstone
    let should_evict_tombstones = payload.dest_level == (config.level_count - 1);

    let mut segment_writer = MultiWriter::new(
        payload.target_size,
        crate::segment::writer::Options {
            block_size: config.block_size,
            evict_tombstones: should_evict_tombstones,
            path: config.path.join(SEGMENTS_FOLDER),
        },
    )?;

    for (idx, item) in merge_iter.enumerate() {
        segment_writer.write(item?)?;

        if idx % 100_000 == 0 && stop_signal.is_stopped() {
            log::debug!("compaction thread: stopping amidst compaction because of stop signal");
            return Ok(());
        }
    }

    let created_segments = segment_writer.finish()?;

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
                descriptor_table: Arc::clone(&descriptor_table),
                metadata,
                block_cache: Arc::clone(block_cache),
                block_index: BlockIndex::from_file(
                    segment_id,
                    descriptor_table,
                    path,
                    Arc::clone(block_cache),
                )?
                .into(),
            })
        })
        .collect::<crate::Result<Vec<_>>>()?;

    log::debug!("compaction: acquiring levels manifest write lock");
    let mut segments_lock = levels.write().expect("lock is poisoned");

    log::debug!(
        "Compacted in {}ms ({} segments created)",
        start.elapsed().as_millis(),
        created_segments.len()
    );

    // NOTE: Write lock memtable, otherwise segments may get deleted while a range read is happening
    log::debug!("compaction: acquiring immu memtables write lock");
    let memtable_lock = immutable_memtables.write().expect("lock is poisoned");

    for segment in created_segments {
        log::trace!("Persisting segment {}", segment.metadata.id);
        segments_lock.insert_into_level(payload.dest_level, segment.into());
    }

    for key in &payload.segment_ids {
        log::trace!("Removing segment {}", key);
        segments_lock.remove(key);
    }

    // NOTE: This is really important
    // Write the segment with the removed segments first
    // Otherwise the folder is deleted, but the segment is still referenced!
    segments_lock.write_to_disk()?;

    for key in &payload.segment_ids {
        log::trace!("rm -rf segment folder {}", key);
        std::fs::remove_dir_all(config.path.join(SEGMENTS_FOLDER).join(key))?;
    }

    segments_lock.show_segments(&payload.segment_ids);

    drop(memtable_lock);
    drop(segments_lock);
    log::debug!("Compaction successful");

    Ok(())
}

pub fn compaction_worker(
    config: &Config,
    levels: &Arc<RwLock<Levels>>,
    stop_signal: &StopSignal,
    compaction_strategy: &Arc<dyn CompactionStrategy + Send + Sync>,
    immutable_memtables: &Arc<RwLock<BTreeMap<String, Arc<MemTable>>>>,
    open_snapshots: &Arc<AtomicU32>,
    block_cache: &Arc<BlockCache>,
) -> crate::Result<()> {
    loop {
        log::debug!("compaction: acquiring levels manifest write lock");
        let mut segments_lock = levels.write().expect("lock is poisoned");

        if stop_signal.is_stopped() {
            log::debug!("compaction thread: exiting because of stop signal");
            return Ok(());
        }

        let choice = compaction_strategy.choose(&segments_lock, config);

        match choice {
            Choice::DoCompact(payload) => {
                drop(segments_lock);

                do_compaction(
                    config,
                    levels,
                    stop_signal,
                    immutable_memtables,
                    open_snapshots,
                    block_cache,
                    &payload,
                )?;
            }
            Choice::DeleteSegments(payload) => {
                // NOTE: Write lock memtable, otherwise segments may get deleted while a range read is happening
                log::debug!("compaction: acquiring immu memtables write lock");
                let _memtable_lock = immutable_memtables.write().expect("lock is poisoned");

                for key in &payload {
                    log::trace!("Removing segment {}", key);
                    segments_lock.remove(key);
                }

                // NOTE: This is really important
                // Write the segment with the removed segments first
                // Otherwise the folder is deleted, but the segment is still referenced!
                segments_lock.write_to_disk()?;

                for key in &payload {
                    log::trace!("rm -rf segment folder {}", key);
                    std::fs::remove_dir_all(config.path.join(SEGMENTS_FOLDER).join(key))?;
                }

                log::trace!("Deleted {} segments", payload.len());
            }
            Choice::DoNothing => {
                log::trace!("Compactor chose to do nothing");
                return Ok(());
            }
        }
    }
}

pub fn start_compaction_thread(tree: &Tree) -> std::thread::JoinHandle<crate::Result<()>> {
    let config = tree.config();
    let levels = Arc::clone(&tree.levels);
    let stop_signal = tree.stop_signal.clone();
    let compaction_strategy = Arc::clone(&config.compaction_strategy);
    let immutable_memtables = Arc::clone(&tree.immutable_memtables);
    let open_snapshots = Arc::clone(&tree.open_snapshots);
    let block_cache = Arc::clone(&tree.block_cache);

    let compaction_semaphore = Arc::clone(&tree.compaction_semaphore);

    std::thread::spawn(move || {
        // TODO: maybe change compaction semaphore to atomic u8
        // TODO: when there are already N compaction threads running, just don't spawn a thread
        compaction_semaphore.acquire();

        compaction_worker(
            &config,
            &levels,
            &stop_signal,
            &compaction_strategy,
            &immutable_memtables,
            &open_snapshots,
            &block_cache,
        )?;

        log::trace!("Post compaction semaphore");
        compaction_semaphore.release();

        Ok(())
    })
}
