// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::Journal;
use std::path::{Path, PathBuf};

pub type JournalId = u64;

#[derive(Debug)]
#[allow(clippy::module_name_repetitions)]
pub struct RecoveryResult {
    pub(crate) active: Journal,
    pub(crate) sealed: Vec<(JournalId, PathBuf)>,
    pub(crate) was_active_created: bool,
}

pub fn recover_journals<P: AsRef<Path>>(path: P) -> crate::Result<RecoveryResult> {
    let path = path.as_ref();

    let mut max_journal_id: JournalId = 0;
    let mut journal_fragments = Vec::<(JournalId, PathBuf)>::new();

    for dirent in std::fs::read_dir(path)? {
        let dirent = dirent?;
        let path = dirent.path();
        let filename = dirent.file_name();

        // https://en.wikipedia.org/wiki/.DS_Store
        if filename == ".DS_Store" {
            continue;
        }

        // https://en.wikipedia.org/wiki/AppleSingle_and_AppleDouble_formats
        if filename.to_string_lossy().starts_with("._") {
            continue;
        }

        assert!(dirent.file_type()?.is_file());

        let filename = filename.to_str().expect("should be utf-8");

        let journal_id = filename
            .strip_suffix(".sealed") // TODO: 3.0.0 remove in V3
            .unwrap_or(filename)
            .parse::<JournalId>()
            .inspect_err(|e| {
                log::error!("found an invalid journal file name {filename:?}: {e:?}");
            })
            .expect("should be a valid journal file name");

        max_journal_id = max_journal_id.max(journal_id);

        journal_fragments.push((journal_id, path));
    }

    // NOTE: Sort ascending, so the last item is the active journal
    journal_fragments.sort_by(|(a, _), (b, _)| a.cmp(b));

    log::trace!("Recovered {journal_fragments:#?}");

    Ok(match journal_fragments.pop() {
        Some((_, active)) => RecoveryResult {
            active: Journal::from_file(active)?,
            sealed: journal_fragments,
            was_active_created: false,
        },
        None => RecoveryResult {
            active: {
                let id: JournalId = max_journal_id + 1;
                Journal::create_new(path.join(id.to_string()))?
            },
            sealed: vec![],
            was_active_created: true,
        },
    })
}
