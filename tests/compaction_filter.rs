use fjall::{DurabilityLevel::Buffer, KeyspaceCreateOptions};
use lsm_tree::compaction::filter::{
    CompactionFilter, Context as CompactionFilterContext, Factory, ItemAccessor, Verdict,
};
use lsm_tree::get_tmp_folder;
use std::sync::Arc;
use test_log::test;

/// Only keeps KVs that start with "a"
struct AFilter;

impl CompactionFilter for AFilter {
    fn filter_item(
        &mut self,
        item: ItemAccessor<'_>,
        _ctx: &CompactionFilterContext,
    ) -> lsm_tree::Result<Verdict> {
        if item.key().starts_with(b"a") {
            Ok(Verdict::Keep)
        } else {
            Ok(Verdict::Remove)
        }
    }
}

struct MyFactory;

impl Factory for MyFactory {
    fn name(&self) -> &str {
        "A"
    }

    fn make_filter(&self, _ctx: &CompactionFilterContext) -> Box<dyn CompactionFilter> {
        Box::new(AFilter)
    }
}

#[test]
fn compaction_filter() -> fjall::Result<()> {
    let folder = get_tmp_folder();

    let db = fjall::Database::builder(&folder)
        .worker_threads_unchecked(0)
        .with_compaction_filters(|keyspace| match keyspace {
            "my_items" => Some(Arc::new(MyFactory)),
            _ => None,
        })
        .open()?;

    {
        let tree = db.keyspace("my_items", KeyspaceCreateOptions::default)?;

        tree.insert("a", "a", Buffer)?;
        tree.flush(true)?;
        tree.insert("abc", "abc", Buffer)?;
        tree.insert("b", "b", Buffer)?;
        tree.flush(true)?;

        assert!(tree.contains_key("a")?);
        assert!(tree.contains_key("abc")?);
        assert!(tree.contains_key("b")?);

        tree.major_compact()?;

        assert!(tree.contains_key("a")?);
        assert!(tree.contains_key("abc")?);
        assert!(!tree.contains_key("b")?);
    }

    {
        let tree = db.keyspace("my_items_imposter", KeyspaceCreateOptions::default)?;

        tree.insert("a", "a", Buffer)?;
        tree.flush(true)?;
        tree.insert("abc", "abc", Buffer)?;
        tree.insert("b", "b", Buffer)?;
        tree.flush(true)?;

        assert!(tree.contains_key("a")?);
        assert!(tree.contains_key("abc")?);
        assert!(tree.contains_key("b")?);

        tree.major_compact()?;

        assert!(tree.contains_key("a")?);
        assert!(tree.contains_key("abc")?);
        assert!(tree.contains_key("b")?);
    }

    Ok(())
}

#[test]
fn compaction_filter_recover() -> fjall::Result<()> {
    let folder = get_tmp_folder();

    {
        let db = fjall::Database::builder(&folder)
            .worker_threads_unchecked(0)
            .with_compaction_filters(|keyspace| match keyspace {
                "my_items" => Some(Arc::new(MyFactory)),
                _ => None,
            })
            .open()?;

        let _tree = db.keyspace("my_items", KeyspaceCreateOptions::default)?;
        let _tree = db.keyspace("my_items_imposter", KeyspaceCreateOptions::default)?;
    }

    {
        let db = fjall::Database::builder(&folder)
            .with_compaction_filters(|keyspace| match keyspace {
                "my_items" => Some(Arc::new(MyFactory)),
                _ => None,
            })
            .open()?;

        {
            let tree = db.keyspace("my_items", KeyspaceCreateOptions::default)?;

            tree.insert("a", "a", Buffer)?;
            tree.flush(true)?;
            tree.insert("abc", "abc", Buffer)?;
            tree.insert("b", "b", Buffer)?;
            tree.flush(true)?;

            assert!(tree.contains_key("a")?);
            assert!(tree.contains_key("abc")?);
            assert!(tree.contains_key("b")?);

            tree.major_compact()?;

            assert!(tree.contains_key("a")?);
            assert!(tree.contains_key("abc")?);
            assert!(!tree.contains_key("b")?);
        }

        {
            let tree = db.keyspace("my_items_imposter", KeyspaceCreateOptions::default)?;

            tree.insert("a", "a", Buffer)?;
            tree.flush(true)?;
            tree.insert("abc", "abc", Buffer)?;
            tree.insert("b", "b", Buffer)?;
            tree.flush(true)?;

            assert!(tree.contains_key("a")?);
            assert!(tree.contains_key("abc")?);
            assert!(tree.contains_key("b")?);

            tree.major_compact()?;

            assert!(tree.contains_key("a")?);
            assert!(tree.contains_key("abc")?);
            assert!(tree.contains_key("b")?);
        }
    }

    Ok(())
}

/// Removes every item
struct RemoveAllFilter;

impl CompactionFilter for RemoveAllFilter {
    fn filter_item(
        &mut self,
        _item: ItemAccessor<'_>,
        _ctx: &CompactionFilterContext,
    ) -> lsm_tree::Result<Verdict> {
        Ok(Verdict::Remove)
    }
}

struct RemoveAllFactory;

impl Factory for RemoveAllFactory {
    fn name(&self) -> &str {
        "remove_all"
    }

    fn make_filter(&self, _ctx: &CompactionFilterContext) -> Box<dyn CompactionFilter> {
        Box::new(RemoveAllFilter)
    }
}

/// e.g. exported by a library that shares the database
fn library_compaction_filters(keyspace: &str) -> Option<Arc<dyn Factory>> {
    match keyspace {
        "library_items" => Some(Arc::new(RemoveAllFactory)),
        _ => None,
    }
}

/// Assigners are asked in the order they were added, the first match wins
#[test]
fn compaction_filter_multiple_assigners() -> fjall::Result<()> {
    let folder = get_tmp_folder();

    {
        let db = fjall::Database::builder(&folder)
            .worker_threads_unchecked(0)
            .open()?;

        for name in ["my_items", "library_items", "other_items"] {
            let tree = db.keyspace(name, KeyspaceCreateOptions::default)?;
            tree.insert("a", "a", Buffer)?;
            tree.insert("b", "b", Buffer)?;
            tree.flush(true)?;
        }
    }

    let db = fjall::Database::builder(&folder)
        .worker_threads_unchecked(0)
        .with_compaction_filters(|keyspace| match keyspace {
            "my_items" => Some(Arc::new(MyFactory)),
            _ => None,
        })
        .with_compaction_filters(library_compaction_filters)
        // NOTE: Never used, "my_items" is already claimed by the first assigner
        .with_compaction_filters(|keyspace| match keyspace {
            "my_items" => Some(Arc::new(RemoveAllFactory)),
            _ => None,
        })
        .open()?;

    let tree = db.keyspace("my_items", KeyspaceCreateOptions::default)?;
    tree.major_compact()?;
    assert!(tree.contains_key("a")?);
    assert!(!tree.contains_key("b")?);

    let tree = db.keyspace("library_items", KeyspaceCreateOptions::default)?;
    tree.major_compact()?;
    assert!(tree.is_empty()?);

    let tree = db.keyspace("other_items", KeyspaceCreateOptions::default)?;
    tree.major_compact()?;
    assert_eq!(2, tree.len()?);

    Ok(())
}
