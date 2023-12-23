use crate::Keyspace;

/// Runs compaction worker.
///
/// Spawn N of these.
pub fn run(keyspace: &Keyspace) {
    loop {
        keyspace.compaction_manager.wait_for();

        let Some(item) = keyspace.compaction_manager.pop() else {
            continue;
        };

        let strategy = Box::<lsm_tree::compaction::Levelled>::default();
        if let Err(e) = item.tree.compact(strategy) {
            log::error!("Compaction failed: {e:?}");
        };
    }
}
