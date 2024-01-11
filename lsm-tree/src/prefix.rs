use crate::{
    merge::{BoxedIterator, MergeIterator},
    range::MemTableGuard,
    segment::Segment,
    value::{ParsedInternalKey, SeqNo, UserKey, UserValue, ValueType},
    Value,
};
use std::sync::Arc;

pub struct Prefix {
    guard: MemTableGuard,
    prefix: UserKey,
    segments: Vec<Arc<Segment>>,
    seqno: Option<SeqNo>,
}

impl Prefix {
    #[must_use]
    pub fn new(
        guard: MemTableGuard,
        prefix: UserKey,
        segments: Vec<Arc<Segment>>,
        seqno: Option<SeqNo>,
    ) -> Self {
        Self {
            guard,
            prefix,
            segments,
            seqno,
        }
    }
}

#[allow(clippy::module_name_repetitions)]
pub struct PrefixIterator<'a> {
    iter: BoxedIterator<'a>,
}

impl<'a> PrefixIterator<'a> {
    fn new(lock: &'a Prefix, seqno: Option<SeqNo>) -> Self {
        let mut segment_iters: Vec<BoxedIterator<'a>> = vec![];

        for segment in &lock.segments {
            let reader = segment.prefix(lock.prefix.clone());

            segment_iters.push(Box::new(reader));
        }

        let mut iters: Vec<BoxedIterator<'a>> = vec![Box::new(MergeIterator::new(segment_iters))];

        for (_, memtable) in lock.guard.sealed.iter() {
            iters.push(Box::new(
                memtable
                    .items
                    // NOTE: See memtable.rs for range explanation
                    .range(
                        ParsedInternalKey::new(
                            lock.prefix.clone(),
                            SeqNo::MAX,
                            ValueType::Tombstone,
                        )..,
                    )
                    .filter(|entry| entry.key().user_key.starts_with(&lock.prefix))
                    .map(|entry| Ok(Value::from((entry.key().clone(), entry.value().clone())))),
            ));
        }

        let memtable_iter = {
            lock.guard
                .active
                .items
                .range(
                    ParsedInternalKey::new(lock.prefix.clone(), SeqNo::MAX, ValueType::Tombstone)..,
                )
                .filter(|entry| entry.key().user_key.starts_with(&lock.prefix))
                .map(|entry| Ok(Value::from((entry.key().clone(), entry.value().clone()))))
        };

        iters.push(Box::new(memtable_iter));

        let mut iter = MergeIterator::new(iters).evict_old_versions(true);

        if let Some(seqno) = seqno {
            iter = iter.snapshot_seqno(seqno);
        }

        let iter = Box::new(iter.filter(|x| match x {
            Ok(value) => value.value_type != ValueType::Tombstone,
            Err(_) => true,
        }));

        Self { iter }
    }
}

impl<'a> Iterator for PrefixIterator<'a> {
    type Item = crate::Result<(UserKey, UserValue)>;

    fn next(&mut self) -> Option<Self::Item> {
        Some(self.iter.next()?.map(|x| (x.key, x.value)))
    }
}

impl<'a> DoubleEndedIterator for PrefixIterator<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        Some(self.iter.next_back()?.map(|x| (x.key, x.value)))
    }
}

impl<'a> IntoIterator for &'a Prefix {
    type IntoIter = PrefixIterator<'a>;
    type Item = <Self::IntoIter as Iterator>::Item;

    fn into_iter(self) -> Self::IntoIter {
        PrefixIterator::new(self, self.seqno)
    }
}
