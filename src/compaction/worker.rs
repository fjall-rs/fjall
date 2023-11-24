use crate::{
    compaction::Choice,
    merge::MergeIterator,
    segment::{index::MetaIndex, writer::MultiWriter, Segment},
    Tree,
};
use std::{sync::Arc, time::Instant};

pub fn compaction_worker(tree: &Tree) -> crate::Result<()> {
    tree.compaction_semaphore.acquire();

    log::debug!("Compaction thread received event: New segment flushed");
    let start = Instant::now();

    log::trace!("Acquiring segment lock");
    let mut segments_lock = tree.levels.write().expect("lock poisoned");

    match tree.config.compaction_strategy.choose(&segments_lock) {
        Choice::DoCompact(payload) => {
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

                MergeIterator::from_segments(&to_merge)
                    .expect("Failed to create interleave iterator")
            };

            segments_lock.hide_segments(&payload.segment_ids);
            drop(segments_lock);
            log::trace!("Freed segment lock");

            let mut segment_writer = MultiWriter::new(
                payload.target_size,
                crate::segment::writer::Options {
                    block_size: tree.config.block_size,
                    evict_tombstones: false, /* TODO: for last level */
                    path: tree.path().join("segments"),
                },
            )?;

            for item in merge_iter {
                segment_writer.write(item?)?;
            }

            let created_segments = segment_writer.finish()?;

            debug_assert!(!created_segments.is_empty());

            for metadata in &created_segments {
                metadata.write_to_file()?;
            }

            let created_segments = created_segments
                .into_iter()
                .map(|metadata| -> crate::Result<Segment> {
                    let segment_id = metadata.id.clone();
                    let path = metadata.path.clone();

                    Ok(Segment {
                        metadata,
                        block_cache: Arc::clone(&tree.block_cache),
                        block_index: MetaIndex::from_file(
                            segment_id,
                            path,
                            Arc::clone(&tree.block_cache),
                        )
                        .unwrap()
                        .into(),
                    })
                })
                .collect::<crate::Result<Vec<_>>>()?;

            log::trace!("Acquiring segment lock");
            let mut segments_lock = tree.levels.write().expect("lock poisoned");

            log::debug!(
                "Compacted in {}ms ({} segments created)",
                start.elapsed().as_millis(),
                created_segments.len()
            );

            // NOTE: Write lock memtable, otherwise segments may get deleted while a range read is happening
            log::trace!("Acquiring memtable lock");
            let memtable_lock = tree.immutable_memtables.write().expect("lock poisoned");

            for segment in created_segments {
                log::trace!("Persisting segment {}", segment.metadata.id);
                segments_lock.insert_into_level(payload.dest_level, segment.into());
            }

            for key in &payload.segment_ids {
                log::trace!("Removing segment {}", key);
                segments_lock.remove(key);
            }

            for key in &payload.segment_ids {
                log::trace!("rm -rf segment folder {}", key);
                std::fs::remove_dir_all(tree.path().join("segments").join(key))?;
            }

            segments_lock.write_to_disk()?;
            segments_lock.show_segments(&payload.segment_ids);

            drop(memtable_lock);
            drop(segments_lock);
            log::debug!("Compaction successful");
        }
        Choice::DoNothing => {
            log::trace!("Compactor chose to do nothing");
            drop(segments_lock);
            log::trace!("Compaction: Freed segment lock");
        }
    }

    Ok(())
}

pub fn start_compaction_thread(
    tree: &Tree,
) -> crate::Result<std::thread::JoinHandle<crate::Result<()>>> {
    let tree = tree.clone();
    Ok(std::thread::spawn(move || compaction_worker(&tree)))
}
