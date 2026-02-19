use fjall::KeyspaceCreateOptions;
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
        .with_compaction_filter_factories(&|keyspace| match keyspace {
            "my_items" => Some(Arc::new(MyFactory)),
            _ => None,
        })
        .open()?;

    {
        let tree = db.keyspace("my_items", KeyspaceCreateOptions::default)?;

        tree.insert("a", "a")?;
        tree.rotate_memtable_and_wait()?;
        tree.insert("abc", "abc")?;
        tree.insert("b", "b")?;
        tree.rotate_memtable_and_wait()?;

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

        tree.insert("a", "a")?;
        tree.rotate_memtable_and_wait()?;
        tree.insert("abc", "abc")?;
        tree.insert("b", "b")?;
        tree.rotate_memtable_and_wait()?;

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
            .with_compaction_filter_factories(&|keyspace| match keyspace {
                "my_items" => Some(Arc::new(MyFactory)),
                _ => None,
            })
            .open()?;

        let _tree = db.keyspace("my_items", KeyspaceCreateOptions::default)?;
        let _tree = db.keyspace("my_items_imposter", KeyspaceCreateOptions::default)?;
    }

    {
        let db = fjall::Database::builder(&folder)
            .with_compaction_filter_factories(&|keyspace| match keyspace {
                "my_items" => Some(Arc::new(MyFactory)),
                _ => None,
            })
            .open()?;

        {
            let tree = db.keyspace("my_items", KeyspaceCreateOptions::default)?;

            tree.insert("a", "a")?;
            tree.rotate_memtable_and_wait()?;
            tree.insert("abc", "abc")?;
            tree.insert("b", "b")?;
            tree.rotate_memtable_and_wait()?;

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

            tree.insert("a", "a")?;
            tree.rotate_memtable_and_wait()?;
            tree.insert("abc", "abc")?;
            tree.insert("b", "b")?;
            tree.rotate_memtable_and_wait()?;

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
