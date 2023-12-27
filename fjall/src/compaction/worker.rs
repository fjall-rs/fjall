use crate::Keyspace;

/// Runs compaction worker.
///
/// Spawn N of these.
pub fn run(keyspace: &Keyspace) {
    loop {
        // TODO: stop signal

        // TODO: if there are no more tasks, this thread may wait forever
        keyspace.compaction_manager.wait_for();

        let Some(item) = keyspace.compaction_manager.pop() else {
            continue;
        };

        // TODO: stop signal

        let strategy = item.compaction_strategy.clone();

        if let Err(e) = item.tree.compact(strategy) {
            log::error!("Compaction failed: {e:?}");
        };
    }
}
