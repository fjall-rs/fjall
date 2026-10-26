use fjall::{Database, DurabilityLevel::Buffer, KeyspaceCreateOptions};
use test_log::test;

#[test]
fn recover_from_different_folder() -> fjall::Result<()> {
    if std::path::Path::new(".test").try_exists()? {
        std::fs::remove_dir_all(".test")?;
    }

    let folder = ".test/asd";

    {
        let db = Database::builder(folder).open()?;
        let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;

        tree.insert("abc", "def", Buffer)?;
        tree.insert("wqewe", "def", Buffer)?;
        tree.insert("ewewq", "def", Buffer)?;
        tree.insert("asddas", "def", Buffer)?;
        tree.insert("ycxycx", "def", Buffer)?;
        tree.insert("asdsda", "def", Buffer)?;
        tree.insert("wewqe", "def", Buffer)?;
    }

    let absolute_folder = std::path::Path::new(folder).canonicalize()?;

    std::fs::create_dir_all(".test/def")?;
    std::env::set_current_dir(".test/def")?;

    for _ in 0..100 {
        let _db = Database::builder(&absolute_folder).open()?;
    }

    Ok(())
}
