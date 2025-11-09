use fjall::Database;
use test_log::test;

#[test]
fn recover_from_different_folder() -> fjall::Result<()> {
    if std::path::Path::new(".test").try_exists()? {
        std::fs::remove_dir_all(".test")?;
    }

    let folder = ".test/asd";

    {
        let db = Database::builder(folder).open()?;
        let tree = db.keyspace("default", Default::default())?;

        tree.insert("abc", "def")?;
        tree.insert("wqewe", "def")?;
        tree.insert("ewewq", "def")?;
        tree.insert("asddas", "def")?;
        tree.insert("ycxycx", "def")?;
        tree.insert("asdsda", "def")?;
        tree.insert("wewqe", "def")?;
    }

    let absolute_folder = std::path::Path::new(folder).canonicalize()?;

    std::fs::create_dir_all(".test/def")?;
    std::env::set_current_dir(".test/def")?;

    for _ in 0..100 {
        let _db = Database::builder(&absolute_folder)
            .max_write_buffer_size(1_024 * 1_024)
            .open()?;
    }

    Ok(())
}
