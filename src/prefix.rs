use crate::{
    merge::{BoxedIterator, MergeIterator},
    range::MemTableGuard,
    segment::Segment,
    value::{ParsedInternalKey, SeqNo},
    Value,
};
use std::sync::Arc;

pub struct Prefix<'a> {
    guard: MemTableGuard<'a>,
    prefix: Vec<u8>,
    segments: Vec<Arc<Segment>>,
}

impl<'a> Prefix<'a> {
    pub fn new(guard: MemTableGuard<'a>, prefix: Vec<u8>, segments: Vec<Arc<Segment>>) -> Self {
        Self {
            guard,
            prefix,
            segments,
        }
    }
}

#[allow(clippy::module_name_repetitions)]
pub struct PrefixIterator<'a> {
    iter: BoxedIterator<'a>,
}

impl<'a> PrefixIterator<'a> {
    fn new(lock: &'a Prefix<'a>) -> Self {
        let mut segment_iters: Vec<BoxedIterator<'a>> = vec![];

        for segment in &lock.segments {
            let reader = segment
                .prefix(lock.prefix.clone())
                .expect("failed to init prefix iter");

            segment_iters.push(Box::new(reader));
        }

        let mut iters: Vec<BoxedIterator<'a>> = vec![Box::new(MergeIterator::new(segment_iters))];

        for (_, memtable) in lock.guard.immutable.iter() {
            iters.push(Box::new(
                memtable
                    .items
                    // NOTE: See memtable.rs for range explanation
                    .range(ParsedInternalKey::new(lock.prefix.clone(), SeqNo::MAX, true)..)
                    .filter(|entry| entry.key().user_key.starts_with(&lock.prefix))
                    .map(|entry| Ok(Value::from((entry.key().clone(), entry.value().clone())))),
            ));
        }

        let memtable_iter = {
            lock.guard
                .active
                .items
                .range(ParsedInternalKey::new(lock.prefix.clone(), SeqNo::MAX, true)..)
                .filter(|entry| entry.key().user_key.starts_with(&lock.prefix))
                .map(|entry| Ok(Value::from((entry.key().clone(), entry.value().clone()))))
        };

        iters.push(Box::new(memtable_iter));

        let iter = Box::new(MergeIterator::new(iters).evict_old_versions(true).filter(
            |x| match x {
                Ok(value) => !value.is_tombstone,
                Err(_) => true,
            },
        ));

        Self { iter }
    }
}

impl<'a> Iterator for PrefixIterator<'a> {
    type Item = crate::Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        Some(self.iter.next()?.map(|x| (x.key, x.value)))
    }
}

impl<'a> DoubleEndedIterator for PrefixIterator<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        Some(self.iter.next_back()?.map(|x| (x.key, x.value)))
    }
}

impl<'a> IntoIterator for &'a Prefix<'a> {
    type IntoIter = PrefixIterator<'a>;
    type Item = <Self::IntoIter as Iterator>::Item;

    fn into_iter(self) -> Self::IntoIter {
        PrefixIterator::new(self)
    }
}
