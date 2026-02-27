use fjall::compaction::filter::*;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Deserialize, Serialize, Eq, PartialEq)]
struct RecordV1 {
    id: u64,
    deprecated_column: String,
}

#[derive(Debug, Deserialize, Serialize, Eq, PartialEq)]
struct RecordV2 {
    id: u64,
}

impl From<RecordV1> for RecordV2 {
    fn from(value: RecordV1) -> Self {
        Self { id: value.id }
    }
}

#[derive(Debug, Deserialize, Serialize, Eq, PartialEq)]
enum Record {
    V1(RecordV1),
    V2(RecordV2),
}

struct MyFilter;

impl CompactionFilter for MyFilter {
    fn filter_item(&mut self, item: ItemAccessor<'_>, _ctx: &Context) -> CompactionFilterResult {
        let i = item.value()?;

        let Ok(item) = serde_json::from_slice::<Record>(&i) else {
            // Compaction filters should not panic, so if we cannot deserialize, just return Keep
            return Ok(Verdict::Keep);
        };

        match item {
            Record::V1(v1) => {
                let new_value = RecordV2::from(v1);
                let Ok(new_value) = serde_json::to_string(&Record::V2(new_value)) else {
                    // NOTE: This should really be impossible...
                    return Ok(Verdict::Keep);
                };

                Ok(Verdict::ReplaceValue(new_value.into()))
            }
            Record::V2(_) => Ok(Verdict::Keep),
        }
    }
}

struct MyFactory;

impl Factory for MyFactory {
    fn make_filter(&self, _ctx: &Context) -> Box<dyn CompactionFilter> {
        Box::new(MyFilter)
    }

    fn name(&self) -> &str {
        "migrate_lazy"
    }
}

fn main() -> fjall::Result<()> {
    let db = fjall::Database::builder(".fjall_data")
        .with_compaction_filter_factories({
            Arc::new(move |keyspace| match keyspace {
                "items" => Some(Arc::new(MyFactory)),
                _ => None,
            })
        })
        .temporary(true)
        .open()?;

    let items = db.keyspace("items", fjall::KeyspaceCreateOptions::default)?;

    items.insert(
        "a",
        serde_json::to_string(&Record::V1(RecordV1 {
            id: 1,
            deprecated_column: "hello".to_string(),
        }))
        .unwrap(),
    )?;
    items.insert(
        "b",
        serde_json::to_string(&Record::V1(RecordV1 {
            id: 2,
            deprecated_column: "hello".to_string(),
        }))
        .unwrap(),
    )?;
    items.insert(
        "c",
        serde_json::to_string(&Record::V1(RecordV1 {
            id: 3,
            deprecated_column: "hello".to_string(),
        }))
        .unwrap(),
    )?;
    items.insert("d", "dsadasdasd")?;
    items.insert("e", &[255])?;
    items.rotate_memtable_and_wait()?;

    items.major_compact()?;
    assert_eq!(
        Record::V2(RecordV2 { id: 1 }),
        serde_json::from_slice::<Record>(&items.get("a")?.unwrap()).unwrap(),
    );
    assert_eq!(
        Record::V2(RecordV2 { id: 2 }),
        serde_json::from_slice::<Record>(&items.get("b")?.unwrap()).unwrap(),
    );

    eprintln!("-- ALL GOOD --");

    Ok(())
}
