use fjall::{Database, KeyspaceCreateOptions};
use test_log::test;

// V3 -> V4 migration
#[test]
fn v3_migration() -> fjall::Result<()> {
    let folder = "test_fixture/v3_keyspace";

    let db = Database::builder(folder).open()?;
    let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
    let tree2 = db.keyspace("default2", KeyspaceCreateOptions::default)?;

    /*
        tree.insert("hello", "world")?;
        tree2.insert("hello", "world")?;

        tree.clear()?;
        tree2.clear()?;

        tree.insert("a", "avalue")?;
        tree.insert("b", "bvalue")?;
        tree.insert("c", "cvalue")?;

        tree2.insert("d", "dvalue")?;
    */

    assert!(!tree.contains_key("hello")?);
    assert!(!tree2.contains_key("hello")?);

    assert_eq!(Some(b"avalue".as_slice()), tree.get("a")?.as_deref());
    assert_eq!(Some(b"bvalue".as_slice()), tree.get("b")?.as_deref());
    assert_eq!(Some(b"cvalue".as_slice()), tree.get("c")?.as_deref());

    assert_eq!(Some(b"dvalue".as_slice()), tree2.get("d")?.as_deref());

    Ok(())
}
