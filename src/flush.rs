use crate::{
    compaction::worker::start_compaction_thread,
    id::generate_segment_id,
    journal::{mem_table::MemTable, rebuild::rebuild_full_memtable, Journal},
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
    })?;

    log::debug!(
        "Flushing memtable -> {}",
        segment_writer.opts.path.display()
    );

    // TODO: this clone hurts
    for (key, value) in &old_memtable.items {
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

            let created_segment = Segment {
                file: Mutex::new(BufReader::new(File::open(metadata.path.join("blocks"))?)),
                block_index: meta_index,
                block_cache: Arc::clone(&tree.block_cache),
                metadata,
            };

            let mut levels = tree.levels.write().expect("lock is poisoned");
            levels.add(Arc::new(created_segment));
            levels.write_to_disk()?;
            drop(levels);

            log::debug!("Destroying old memtable");
            let mut memtable_lock = tree.immutable_memtables.write().expect("lock poisoned");
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

    // TODO: ArcSwap the journal so we can drop the lock before fully processing the old memtable
    let mut lock = tree.journal.shards.full_lock();

    let old_journal_folder = tree.journal.get_path();

    let segment_id = old_journal_folder
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    let old_memtable = Arc::new(rebuild_full_memtable(&mut lock)?);
    tree.approx_memtable_size_bytes
        .store(0, std::sync::atomic::Ordering::SeqCst);

    let mut immutable_memtables = tree.immutable_memtables.write().expect("lock is poisoned");
    immutable_memtables.insert(segment_id.clone(), Arc::clone(&old_memtable));

    let new_journal_path = tree
        .config
        .path
        .join("journals")
        .join(generate_segment_id());
    Journal::rotate(new_journal_path, &mut lock)?;

    let marker = File::create(old_journal_folder.join(".flush"))?;
    marker.sync_all()?;

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
