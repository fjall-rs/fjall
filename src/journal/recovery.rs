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

    let mut sealed = vec![];
    let mut active = None;
    let mut max_journal_id: JournalId = 0;
    let mut was_active_created = false;

    for dirent in std::fs::read_dir(path)? {
        let dirent = dirent?;
        let path = dirent.path();

        assert!(dirent.file_type()?.is_file());

        let filename = dirent.file_name();
        let filename = filename.to_str().expect("should be utf-8");
        let is_sealed = filename.ends_with(".sealed");

        if is_sealed {
            let journal_id = filename
                .strip_suffix(".sealed")
                .expect("should have suffix")
                .parse::<JournalId>()
                .expect("should be valid journal ID");

            max_journal_id = max_journal_id.max(journal_id);

            sealed.push((journal_id, path));

            continue;
        }

        assert!(active.is_none(), "should only have one active journal");
        active = Some(path);
    }

    sealed.sort_by(|(a, _), (b, _)| a.cmp(b));

    let active = active.map_or_else(
        || {
            was_active_created = true;
            let id: JournalId = max_journal_id + 1;
            Journal::create_new(path.join(id.to_string()))
        },
        Journal::from_file,
    )?;

    Ok(RecoveryResult {
        active,
        sealed,
        was_active_created,
    })
}
