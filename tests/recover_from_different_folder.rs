use fjall::Config;
use test_log::test;

#[test]
fn recover_from_different_folder() -> fjall::Result<()> {
    if std::path::Path::new(".test").try_exists()? {
        std::fs::remove_dir_all(".test")?;
    }

    let folder = ".test/asd";

    {
        let keyspace = Config::new(folder).open()?;
        let partition = keyspace.open_partition("default", Default::default())?;

        partition.insert("abc", "def")?;
        partition.insert("wqewe", "def")?;
        partition.insert("ewewq", "def")?;
        partition.insert("asddas", "def")?;
        partition.insert("ycxycx", "def")?;
        partition.insert("asdsda", "def")?;
        partition.insert("wewqe", "def")?;
    }

    let absolute_folder = std::path::Path::new(folder).canonicalize()?;

    std::fs::create_dir_all(".test/def")?;
    std::env::set_current_dir(".test/def")?;

    for _ in 0..100 {
        let _keyspace = Config::new(&absolute_folder)
            .max_write_buffer_size(1_024 * 1_024)
            .open()?;
    }

    Ok(())
}
