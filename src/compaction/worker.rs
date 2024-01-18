use super::manager::CompactionManager;

/// Runs a single run of compaction.
pub fn run(compaction_manager: &CompactionManager) {
    let Some(item) = compaction_manager.pop() else {
        return;
    };

    log::trace!(
        "compactor: calling compaction strategy for partition {:?}",
        item.0.name
    );
    let strategy = item
        .compaction_strategy
        .read()
        .expect("lock is poisoned")
        .clone();

    // TODO: loop if there's more work to do

    if let Err(e) = item.tree.compact(strategy) {
        log::error!("Compaction failed: {e:?}");
    };
}
