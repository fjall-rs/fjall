use super::manager::CompactionManager;

/// Runs compaction worker.
///
/// Spawn N of these.
pub fn run(compaction_manager: &CompactionManager) {
    let Some(item) = compaction_manager.pop() else {
        return;
    };

    log::info!(
        "compactor: calling compaction strategy for partition {:?}",
        item.0.name
    );
    let strategy = item.compaction_strategy.clone();

    // TODO: loop if there's more work to do

    if let Err(e) = item.tree.compact(strategy) {
        log::error!("Compaction failed: {e:?}");
    };
}
