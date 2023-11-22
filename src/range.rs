use std::{
    collections::BTreeMap,
    ops::Bound,
    path::PathBuf,
    sync::{Arc, RwLockReadGuard},
};

use crate::{
    block_cache::BlockCache, memtable::MemTable, merge::MergeIterator, segment::index::MetaIndex,
    Value,
};

pub struct SegmentInfo {
    pub(crate) id: String,
    pub(crate) path: PathBuf,
    pub(crate) block_index: Arc<MetaIndex>,
}

pub struct MemTableGuard<'a> {
    pub(crate) active: RwLockReadGuard<'a, MemTable>,
    pub(crate) immutable: RwLockReadGuard<'a, BTreeMap<String, Arc<MemTable>>>,
}

pub struct Range<'a> {
    guard: MemTableGuard<'a>,
    bounds: (Bound<Vec<u8>>, Bound<Vec<u8>>),
    segments: Vec<SegmentInfo>,
    block_cache: Arc<BlockCache>,
}

impl<'a> Range<'a> {
    pub fn new(
        guard: MemTableGuard<'a>,
        bounds: (Bound<Vec<u8>>, Bound<Vec<u8>>),
        segments: Vec<SegmentInfo>,
        block_cache: Arc<BlockCache>,
    ) -> Self {
        Self {
            guard,
            bounds,
            segments,
            block_cache,
        }
    }
}

#[allow(clippy::module_name_repetitions)]
pub struct RangeIterator<'a> {
    iter: Box<dyn DoubleEndedIterator<Item = crate::Result<Value>> + 'a>,
}

impl<'a> RangeIterator<'a> {
    fn new(lock: &'a Range<'a>) -> Self {
        let mut segment_iters: Vec<Box<dyn DoubleEndedIterator<Item = crate::Result<Value>> + 'a>> =
            vec![];

        for segment in &lock.segments {
            let range = crate::segment::range::Range::new(
                &segment.path,
                Arc::clone(&segment.block_index),
                lock.bounds.clone(),
                /*   &segment.path,
                segment.id.clone(),
                Arc::clone(&segment.block_index),
                Arc::clone(&lock.block_cache),
                lock.bounds.clone(),  */
            )
            .unwrap();

            segment_iters.push(Box::new(range));
        }

        let mut iters: Vec<Box<dyn DoubleEndedIterator<Item = crate::Result<Value>> + 'a>> =
            vec![Box::new(MergeIterator::new(segment_iters))];

        for (_, memtable) in lock.guard.immutable.iter() {
            iters.push(Box::new(
                memtable
                    .items
                    .range::<Vec<u8>, _>(lock.bounds.clone())
                    .map(|(_, value)| Ok(value.clone())),
            ));
        }

        iters.push(Box::new(
            lock.guard
                .active
                .items
                .range::<Vec<u8>, _>(lock.bounds.clone())
                .map(|(_, value)| Ok(value.clone())),
        ));

        let iter = Box::new(MergeIterator::new(iters).filter(|x| match x {
            Ok(value) => !value.is_tombstone,
            Err(_) => true,
        }));

        Self { iter }
    }
}

impl<'a> Iterator for RangeIterator<'a> {
    type Item = crate::Result<Value>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

impl<'a> DoubleEndedIterator for RangeIterator<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.iter.next_back()
    }
}

impl<'a> IntoIterator for &'a Range<'a> {
    type IntoIter = RangeIterator<'a>;
    type Item = <Self::IntoIter as Iterator>::Item;

    fn into_iter(self) -> Self::IntoIter {
        RangeIterator::new(self)
    }
}
