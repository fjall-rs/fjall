// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{snapshot_nonce::SnapshotNonce, Guard};

type InnerIter = Box<dyn DoubleEndedIterator<Item = lsm_tree::IterGuardImpl> + Send + 'static>;

/// A wrapper around iterators that keep a snapshot alive
//
// We need to hold the snapshot nonce so the GC watermark does not
// move past this snapshot nonce, removing data that may still be read.
//
// Additionally, this struct also maps lsm-tree's Guards to "our" Guards.
pub struct Iter {
    iter: InnerIter,

    #[expect(unused)]
    nonce: SnapshotNonce,
}

impl Iter {
    pub(crate) fn new(nonce: SnapshotNonce, iter: InnerIter) -> Self {
        Self { iter, nonce }
    }
}

impl Iterator for Iter {
    type Item = crate::Guard;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(Guard)
    }
}

impl DoubleEndedIterator for Iter {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.iter.next_back().map(Guard)
    }
}
