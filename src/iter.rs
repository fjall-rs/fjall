// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::snapshot_nonce::SnapshotNonce;

/// A wrapper around iterators to hold a snapshot moment
///
/// We need to hold the snapshot nonce so the GC watermark does not
/// move past this snapshot moment, removing data that may still be read.
///
/// This may not be strictly needed because an iterator holds a read lock to a memtable anyway
/// but for correctness it's probably better.
pub struct Iter<T, I: DoubleEndedIterator<Item = crate::Result<T>>> {
    iter: I,

    #[allow(unused)]
    nonce: SnapshotNonce,
}

impl<T, I: DoubleEndedIterator<Item = crate::Result<T>>> Iter<T, I> {
    pub fn new(nonce: SnapshotNonce, iter: I) -> Self {
        Self { iter, nonce }
    }
}

impl<T, I: DoubleEndedIterator<Item = crate::Result<T>>> Iterator for Iter<T, I> {
    type Item = crate::Result<T>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

impl<T, I: DoubleEndedIterator<Item = crate::Result<T>>> DoubleEndedIterator for Iter<T, I> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.iter.next_back()
    }
}
