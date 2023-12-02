use crate::{
    merge::{BoxedIterator, MergeIterator},
    Value,
};

use super::{mem_table::MemTable, shard::JournalShard};
use std::sync::RwLockWriteGuard;

pub fn rebuild_full_memtable<'a>(
    mut full_lock: &mut Vec<RwLockWriteGuard<'a, JournalShard>>,
) -> crate::Result<MemTable> {
    let mut mega_table = MemTable::default();

    let memtable_iter = {
        let mut iters: Vec<BoxedIterator<'a>> = vec![];

        for shard in full_lock {
            let tree = std::mem::take(&mut shard.memtable.items);

            let iter = tree
                .into_iter()
                .map(|(key, value)| Ok(Value::from((key, value))));

            iters.push(Box::new(iter));
        }

        MergeIterator::new(iters)
    };

    for item in memtable_iter {
        let item = item?;
        mega_table.insert(item);
    }

    Ok(mega_table)
}
