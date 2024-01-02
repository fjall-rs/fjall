use super::manager::CompactionManager;

/// Runs compaction worker.
///
/// Spawn N of these.
pub fn run(compaction_manager: &CompactionManager) {
    let Some(item) = compaction_manager.pop() else {
        return;
    };

    log::trace!("compactor: calling compaction strategy");
    let strategy = item.compaction_strategy.clone();

    if let Err(e) = item.tree.compact(strategy) {
        log::error!("Compaction failed: {e:?}");
    };
}
