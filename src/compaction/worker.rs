use crate::{
    compaction::Choice,
    descriptor_table::FileDescriptorTable,
    levels::Levels,
    merge::MergeIterator,
    segment::{index::MetaIndex, writer::MultiWriter, Segment},
    Tree,
};
use std::{
    sync::{Arc, RwLockWriteGuard},
    time::Instant,
};

pub fn do_compaction(
    tree: &Tree,
    payload: &crate::compaction::Input,
    mut segments_lock: RwLockWriteGuard<'_, Levels>,
) -> crate::Result<()> {
    if tree.is_stopped() {
        log::debug!("Got stop signal: compaction thread is stopping");
        return Ok(());
    }

    let start = Instant::now();

    log::debug!(
        "Chosen {} segments to compact into a single new segment at level {}",
        payload.segment_ids.len(),
        payload.dest_level
    );

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
        let snapshot_count = tree
            .open_snapshots
            .load(std::sync::atomic::Ordering::Acquire);
        let no_snapshots_open = snapshot_count == 0;

        MergeIterator::from_segments(&to_merge)?.evict_old_versions(no_snapshots_open)
    };

    segments_lock.hide_segments(&payload.segment_ids);
    drop(segments_lock);
    log::trace!("Freed segment lock");

    // NOTE: Only evict tombstones when reaching the last level,
    // That way we don't resurrect data beneath the tombstone
    let should_evict_tombstones = payload.dest_level == (tree.config.levels - 1);

    let mut segment_writer = MultiWriter::new(
        payload.target_size,
        crate::segment::writer::Options {
            block_size: tree.config.block_size,
            evict_tombstones: should_evict_tombstones,
            path: tree.config().path.join("segments"),
        },
    )?;

    for (idx, item) in merge_iter.enumerate() {
        segment_writer.write(item?)?;

        if idx % 100_000 == 0 && tree.is_stopped() {
            log::debug!("Got stop signal: compaction thread is stopping");
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

            Ok(Segment {
                descriptor_table: Arc::new(FileDescriptorTable::new(path.join("blocks"))?),
                metadata,
                block_cache: Arc::clone(&tree.block_cache),
                block_index: MetaIndex::from_file(segment_id, path, Arc::clone(&tree.block_cache))?
                    .into(),
            })
        })
        .collect::<crate::Result<Vec<_>>>()?;

    log::trace!("Acquiring segment lock");
    let mut segments_lock = tree.levels.write().expect("lock is poisoned");

    log::debug!(
        "Compacted in {}ms ({} segments created)",
        start.elapsed().as_millis(),
        created_segments.len()
    );

    // NOTE: Write lock memtable, otherwise segments may get deleted while a range read is happening
    log::trace!("Acquiring memtable lock");
    let memtable_lock = tree.immutable_memtables.write().expect("lock is poisoned");

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
        std::fs::remove_dir_all(tree.config().path.join("segments").join(key))?;
    }

    segments_lock.show_segments(&payload.segment_ids);

    drop(memtable_lock);
    drop(segments_lock);
    log::debug!("Compaction successful");

    Ok(())
}

pub fn compaction_worker(tree: &Tree) -> crate::Result<()> {
    loop {
        log::trace!("Acquiring segment lock");
        let mut segments_lock = tree.levels.write().expect("lock is poisoned");

        if tree.is_stopped() {
            log::debug!("Got stop signal: compaction thread is stopping");
            return Ok(());
        }

        match tree.config.compaction_strategy.choose(&segments_lock) {
            Choice::DoCompact(payload) => {
                do_compaction(tree, &payload, segments_lock)?;
            }
            Choice::DeleteSegments(payload) => {
                // NOTE: Write lock memtable, otherwise segments may get deleted while a range read is happening
                log::trace!("Acquiring memtable lock");
                let _memtable_lock = tree.immutable_memtables.write().expect("lock is poisoned");

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
                    std::fs::remove_dir_all(tree.config().path.join("segments").join(key))?;
                }

                log::trace!("Deleted {} segments", payload.len());
            }
            Choice::DoNothing => {
                log::trace!("Compactor chose to do nothing");
                drop(segments_lock);
                log::trace!("Compaction: Freed segment lock");
                return Ok(());
            }
        }
    }
}

pub fn start_compaction_thread(tree: &Tree) -> std::thread::JoinHandle<crate::Result<()>> {
    let tree = tree.clone();

    std::thread::spawn(move || {
        // TODO: maybe change compaction semaphore to atomic u8
        // TODO: when there are already N compaction threads running, just don't spawn a thread
        tree.compaction_semaphore.acquire();

        log::debug!("Compaction thread received event: New segment flushed");
        compaction_worker(&tree)?;

        log::trace!("Post compaction semaphore");
        tree.compaction_semaphore.release();

        Ok(())
    })
}
