use crate::{
    bloom::BloomFilter,
    compaction::worker::start_compaction_thread,
    id::generate_segment_id,
    journal::Journal,
    memtable::MemTable,
    segment::{index::MetaIndex, meta::Metadata, writer::Writer, Segment},
    Tree,
};
use std::{
    fs::File,
    io::BufReader,
    path::Path,
    sync::{Arc, Mutex},
};

fn flush_worker(
    tree: &Tree,
    old_memtable: &Arc<MemTable>,
    segment_id: &str,
    old_journal_folder: &Path,
) -> crate::Result<()> {
    let segment_folder = tree.config.path.join("segments").join(segment_id);

    let mut segment_writer = Writer::new(crate::segment::writer::Options {
        path: segment_folder.clone(),
        evict_tombstones: false,
        block_size: tree.config.block_size,
        approx_item_count: old_memtable.len(),
    })?;

    log::debug!(
        "Flushing memtable -> {}",
        segment_writer.opts.path.display()
    );

    // TODO: this clone hurts
    for entry in &old_memtable.items {
        let key = entry.key();
        let value = entry.value();
        segment_writer.write(crate::Value::from(((key.clone()), value.clone())))?;
    }

    segment_writer.finish()?;
    log::debug!("Finalized segment write");

    let metadata = Metadata::from_writer(segment_id.to_string(), segment_writer);
    metadata.write_to_file()?;

    match MetaIndex::from_file(
        segment_id.into(),
        &segment_folder,
        Arc::clone(&tree.block_cache),
    )
    .map(Arc::new)
    {
        Ok(meta_index) => {
            log::debug!("Read MetaIndex");

            #[cfg(feature = "bloom")]
            let bloom_filter = BloomFilter::from_file(metadata.path.join("bloom"))?;

            let created_segment = Segment {
                file: Mutex::new(BufReader::new(File::open(metadata.path.join("blocks"))?)),
                block_index: meta_index,
                block_cache: Arc::clone(&tree.block_cache),
                metadata,

                #[cfg(feature = "bloom")]
                bloom_filter,
            };

            let mut levels = tree.levels.write().expect("lock is poisoned");
            levels.add(Arc::new(created_segment));
            levels.write_to_disk()?;
            drop(levels);

            log::debug!("Destroying old memtable");
            let mut memtable_lock = tree.immutable_memtables.write().expect("lock is poisoned");
            memtable_lock.remove(segment_id);
            drop(memtable_lock);

            log::debug!(
                "Deleting old journal folder: {}",
                old_journal_folder.display()
            );
            std::fs::remove_dir_all(old_journal_folder)?;
        }
        Err(error) => {
            log::error!("Flush worker error: {:?}", error);
        }
    }

    log::debug!("Flush done");

    Ok(())
}

pub fn start(tree: &Tree) -> crate::Result<std::thread::JoinHandle<crate::Result<()>>> {
    log::trace!("Preparing memtable flush thread");
    tree.flush_semaphore.acquire();
    log::trace!("Got flush semaphore");

    let mut lock = tree.journal.shards.full_lock();
    let mut memtable_lock = tree.active_memtable.write().expect("lock is poisoned");

    if memtable_lock.items.is_empty() {
        log::debug!("MemTable is empty (so another thread beat us to it) - aborting flush");

        // TODO: this is a bit stupid, change it
        return Ok(std::thread::spawn(|| Ok(())));
    }

    let old_journal_folder = lock[0]
        .path
        .parent()
        .expect("journal shard should have parent folder")
        .to_path_buf();

    let segment_id = old_journal_folder
        .file_name()
        .expect("invalid journal folder name")
        .to_str()
        .expect("invalid journal folder name")
        .to_string();

    let old_memtable = std::mem::take(&mut *memtable_lock);
    let old_memtable = Arc::new(old_memtable);

    let mut immutable_memtables = tree.immutable_memtables.write().expect("lock is poisoned");
    immutable_memtables.insert(segment_id.clone(), Arc::clone(&old_memtable));

    drop(memtable_lock);

    log::trace!(
        "Marking journal {} as flushable",
        old_journal_folder.display()
    );

    let marker = File::create(old_journal_folder.join(".flush"))?;
    marker.sync_all()?;

    let folder = File::open(&old_journal_folder)?;
    folder.sync_all()?;

    let new_journal_path = tree
        .config
        .path
        .join("journals")
        .join(generate_segment_id());
    Journal::rotate(new_journal_path, &mut lock)?;

    tree.active_journal_size_bytes
        .store(0, std::sync::atomic::Ordering::Relaxed);

    drop(immutable_memtables);
    drop(lock);

    let tree = tree.clone();

    Ok(std::thread::spawn(move || {
        log::debug!("Starting flush worker");

        if let Err(error) = flush_worker(&tree, &old_memtable, &segment_id, &old_journal_folder) {
            log::error!("Flush thread error: {error:?}");
        };

        log::trace!("Post flush semaphore");
        tree.flush_semaphore.release();

        // Flush done, so notify compaction that a segment was created
        start_compaction_thread(&tree);

        Ok(())
    }))
}
