use lsm_tree::{id::generate_segment_id, SeqNo};
use std::{collections::HashMap, fs::File, io::Write, path::PathBuf, sync::Arc};

use crate::{
    file::{FLUSH_MARKER, FLUSH_PARTITIONS_LIST},
    journal::Journal,
    PartitionHandle,
};

pub struct PartitionSeqno {
    pub(crate) partition: PartitionHandle,
    pub(crate) lsn: SeqNo,
}

impl std::fmt::Debug for PartitionSeqno {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.lsn)
    }
}

pub struct Item {
    pub(crate) path: PathBuf,
    pub(crate) size_in_bytes: u64,
    pub(crate) partition_seqnos: HashMap<Arc<str>, PartitionSeqno>,
}

impl std::fmt::Debug for Item {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "JournalManagerItem {} => {:#?}",
            self.path.display(),
            self.partition_seqnos
        )
    }
}

/// The [`JournalManager`] keeps track of sealed journals that are being flushed.
///
/// Each journal may contain items of different partitions.
pub struct JournalManager {
    journal: Arc<Journal>,
    active_path: PathBuf,
    items: Vec<Item>,
    disk_space_in_bytes: u64,
}

impl JournalManager {
    pub fn new<P: Into<PathBuf>>(journal: Arc<Journal>, path: P) -> Self {
        Self {
            journal,
            active_path: path.into(),
            items: Vec::with_capacity(10),
            disk_space_in_bytes: 0, /* TODO: on recovery set to active journal size */
        }
    }

    /// Returns the amount of bytes used on disk by journals
    pub fn disk_space_used(&self) -> u64 {
        self.disk_space_in_bytes
    }

    /// Performs maintenance, maybe deleting some old journalsPerforms maintenance, maybe deleting some old journals
    pub fn maintenance(&mut self) -> crate::Result<()> {
        // NOTE: Walk backwards because of shifting indices
        'outer: for idx in (0..self.items.len()).rev() {
            let Some(item) = &self.items.get(idx) else {
                continue 'outer;
            };

            for item in item.partition_seqnos.values() {
                let Some(partition_seqno) = item.partition.tree.get_segment_lsn() else {
                    continue 'outer;
                };

                if partition_seqno < item.lsn {
                    continue 'outer;
                }
            }

            // NOTE: Once the LSN of *every* partition's segments [1] is higher than the journal's stored partition seqno,
            // it can be deleted from disk, as we know the entire journal has been flushed to segments [2].
            //
            // [1] We cannot use the partition's max seqno, because the memtable will get writes, which increase the seqno.
            // We *need* to check the disk segments specifically, they are the source of truth for flushed data.
            //
            // [2] Checking the seqno is safe because the queues inside the flush manager are FIFO.
            //
            // IMPORTANT: On recovery, the journals need to be flushed from oldest to newest.
            log::debug!("Removing fully flushed journal at {}", item.path.display());
            std::fs::remove_dir_all(&item.path)?;

            self.disk_space_in_bytes -= item.size_in_bytes;
            self.items.remove(idx);
        }

        Ok(())
    }

    pub fn rotate_journal(
        &mut self,
        seqnos: HashMap<Arc<str>, PartitionSeqno>,
    ) -> crate::Result<()> {
        log::debug!("Sealing journal");

        let old_journal_path = self.active_path.clone();

        let mut file = File::create(old_journal_path.join(FLUSH_PARTITIONS_LIST))?;

        for (name, item) in &seqnos {
            writeln!(file, "{name}:{}", item.lsn)?;
        }
        file.sync_all()?;

        let marker = File::create(old_journal_path.join(FLUSH_MARKER))?;
        marker.sync_all()?;

        #[cfg(not(target_os = "windows"))]
        {
            // Fsync folder on Unix
            let folder = File::open(&old_journal_path)?;
            folder.sync_all()?;
        }

        let new_journal_path = old_journal_path
            .parent()
            .expect("should have parent")
            .join(&*generate_segment_id());

        log::debug!("journal manager: acquiring journal full lock");
        Journal::rotate(
            &new_journal_path,
            &mut self.journal.shards.full_lock().expect("lock is poisoned"),
        )?;

        self.active_path = new_journal_path;

        // TODO: Journal::disk_space

        let journal_size = fs_extra::dir::get_size(&old_journal_path)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("{:?}", e.kind)))?;

        self.disk_space_in_bytes += journal_size;

        self.items.push(Item {
            path: old_journal_path,
            partition_seqnos: seqnos,
            size_in_bytes: journal_size,
        });

        Ok(())
    }
}
