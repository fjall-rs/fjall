use std::{path::PathBuf, sync::Arc};

use crate::{
    block_cache::BlockCache, merge::MergeIterator, range::MemTableGuard, segment::index::MetaIndex,
    Value,
};

pub struct SegmentInfo {
    pub(crate) id: String,
    pub(crate) path: PathBuf,
    pub(crate) block_index: Arc<MetaIndex>,
}

pub struct Prefix<'a> {
    guard: MemTableGuard<'a>,
    prefix: Vec<u8>,
    segments: Vec<SegmentInfo>,
    block_cache: Arc<BlockCache>,
}

impl<'a> Prefix<'a> {
    pub fn new(
        guard: MemTableGuard<'a>,
        prefix: Vec<u8>,
        segments: Vec<SegmentInfo>,
        block_cache: Arc<BlockCache>,
    ) -> Self {
        Self {
            guard,
            prefix,
            segments,
            block_cache,
        }
    }
}

#[allow(clippy::module_name_repetitions)]
pub struct PrefixIterator<'a> {
    iter: Box<dyn DoubleEndedIterator<Item = crate::Result<Value>> + 'a>,
}

impl<'a> PrefixIterator<'a> {
    fn new(lock: &'a Prefix<'a>) -> Self {
        let mut segment_iters: Vec<Box<dyn DoubleEndedIterator<Item = crate::Result<Value>> + 'a>> =
            vec![];

        for segment in &lock.segments {
            segment_iters.push(Box::new(
                crate::segment::prefix::PrefixedReader::new(
                    &segment.path,
                    Arc::clone(&segment.block_index),
                    lock.prefix.clone(),
                )
                .unwrap(),
            ));
        }

        let mut iters: Vec<Box<dyn DoubleEndedIterator<Item = crate::Result<Value>> + 'a>> =
            vec![Box::new(MergeIterator::new(segment_iters))];

        for (_, memtable) in lock.guard.immutable.iter() {
            iters.push(Box::new(
                memtable
                    .items
                    .range::<Vec<u8>, _>(lock.prefix.clone()..)
                    .filter(|(key, _)| key.starts_with(&lock.prefix))
                    .map(|(_, value)| Ok(value.clone())),
            ));
        }

        iters.push(Box::new(
            lock.guard
                .active
                .items
                .range::<Vec<u8>, _>(lock.prefix.clone()..)
                .filter(|(key, _)| key.starts_with(&lock.prefix))
                .map(|(_, value)| Ok(value.clone())),
        ));

        let iter = Box::new(MergeIterator::new(iters).filter(|x| match x {
            Ok(value) => !value.is_tombstone,
            Err(_) => true,
        }));

        Self { iter }
    }
}

impl<'a> Iterator for PrefixIterator<'a> {
    type Item = crate::Result<Value>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

impl<'a> DoubleEndedIterator for PrefixIterator<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.iter.next_back()
    }
}

impl<'a> IntoIterator for &'a Prefix<'a> {
    type IntoIter = PrefixIterator<'a>;
    type Item = <Self::IntoIter as Iterator>::Item;

    fn into_iter(self) -> Self::IntoIter {
        PrefixIterator::new(self)
    }
}
