// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::Journal;
use lsm_tree::CompressionType;
use std::path::{Path, PathBuf};

pub type JournalId = u64;

#[derive(Debug)]
#[allow(clippy::module_name_repetitions)]
pub struct RecoveryResult {
    pub(crate) active: Journal,
    pub(crate) sealed: Vec<(JournalId, PathBuf)>,
    pub(crate) was_active_created: bool,
}

pub fn recover_journals<P: AsRef<Path>>(
    path: P,
    compression: CompressionType,
    compression_threshold: usize,
) -> crate::Result<RecoveryResult> {
    let path = path.as_ref();

    let mut max_journal_id: JournalId = 0;
    let mut journal_fragments = Vec::<(JournalId, PathBuf)>::new();

    log::trace!("Got journal fragments: {journal_fragments:#?}");

    for dirent in std::fs::read_dir(path)? {
        let dirent = dirent?;
        let path = dirent.path();
        let filename = dirent.file_name();

        let Some(filename) = filename.to_str() else {
            log::error!("Invalid journal file name: {}", filename.display());
            return Err(crate::Error::JournalRecovery(
                crate::JournalRecoveryError::InvalidFileName,
            ));
        };

        if !std::path::Path::new(filename)
            .extension()
            .is_some_and(|ext| ext.eq_ignore_ascii_case("jnl"))
        {
            continue;
        }

        assert!(dirent.file_type()?.is_file());

        let Some(basename) = filename.strip_suffix(".jnl") else {
            log::error!("Invalid journal file name: {filename}");
            return Err(crate::Error::JournalRecovery(
                crate::JournalRecoveryError::InvalidFileName,
            ));
        };

        let journal_id = basename.parse::<JournalId>().map_err(|_| {
            log::error!("Invalid journal file name: {filename}");
            crate::Error::JournalRecovery(crate::JournalRecoveryError::InvalidFileName)
        })?;

        max_journal_id = max_journal_id.max(journal_id);

        journal_fragments.push((journal_id, path));
    }

    // NOTE: Sort ascending, so the last item is the active journal
    journal_fragments.sort_by(|(a, _), (b, _)| a.cmp(b));

    log::trace!("Recovered {journal_fragments:#?}");

    Ok(match journal_fragments.pop() {
        Some((_, active)) => RecoveryResult {
            active: Journal::from_file(active)?
                .with_compression(compression, compression_threshold),
            sealed: journal_fragments,
            was_active_created: false,
        },
        None => RecoveryResult {
            active: {
                let id: JournalId = max_journal_id + 1;

                Journal::create_new(path.join(id.to_string()))?
                    .with_compression(compression, compression_threshold)
            },
            sealed: vec![],
            was_active_created: true,
        },
    })
}
