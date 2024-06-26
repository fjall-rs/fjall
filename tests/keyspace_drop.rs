#[cfg(feature = "__internal_integration")]
#[test_log::test]
fn keyspace_drop() -> fjall::Result<()> {
    use fjall::Config;

    {
        let folder = tempfile::tempdir()?;

        assert_eq!(0, fjall::drop::load_drop_counter());
        let keyspace = Config::new(folder).open()?;
        assert_eq!(5, fjall::drop::load_drop_counter());

        drop(keyspace);
        assert_eq!(0, fjall::drop::load_drop_counter());
    }

    {
        let folder = tempfile::tempdir()?;

        assert_eq!(0, fjall::drop::load_drop_counter());
        let keyspace = Config::new(folder).open()?;
        assert_eq!(5, fjall::drop::load_drop_counter());

        let partition = keyspace.open_partition("default", Default::default())?;
        assert_eq!(6, fjall::drop::load_drop_counter());

        drop(partition);
        drop(keyspace);
        assert_eq!(0, fjall::drop::load_drop_counter());
    }

    {
        let folder = tempfile::tempdir()?;

        assert_eq!(0, fjall::drop::load_drop_counter());
        let keyspace = Config::new(folder).open()?;
        assert_eq!(5, fjall::drop::load_drop_counter());

        let partition = keyspace.open_partition("default", Default::default())?;
        assert_eq!(6, fjall::drop::load_drop_counter());

        let partition2 = keyspace.open_partition("different", Default::default())?;
        assert_eq!(7, fjall::drop::load_drop_counter());

        drop(partition2);
        drop(partition);
        drop(keyspace);
        assert_eq!(0, fjall::drop::load_drop_counter());
    }

    Ok(())
}
