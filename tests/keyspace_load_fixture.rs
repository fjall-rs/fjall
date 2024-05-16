use fjall::Config;
use test_log::test;

#[test]
fn keyspace_load_v1() -> fjall::Result<()> {
    let folder = "test_fixture/v1_keyspace";

    let keyspace = Config::new(folder).open()?;

    let a = keyspace.open_partition("a", Default::default())?;
    let b = keyspace.open_partition("b", Default::default())?;
    let c = keyspace.open_partition("c", Default::default())?;

    /*   a.insert("a", "Only ever feeling fine")?;
    a.insert("b", "And I'd prefer us to be close")?;
    a.insert("c", "I'd like to look you in the eyes, not fearin'")?;
    a.insert("d", "Starin' into the void, waitin' for replies")?;
    a.insert("e", "Just waitin' for replies")?;
    a.insert("f", "---")?;
    a.insert("g", "Did you actually load this database?")?;
    a.insert("h", "https://www.youtube.com/watch?v=eYS7xmjR4Fk")?;

    a.rotate_memtable()?;

    b.insert("a", "Whisper")?;
    b.insert("b", "Answers")?;
    b.insert("c", "There's no one")?;
    b.insert("d", "Out there")?;

    b.rotate_memtable()?;

    c.insert("a", "Whisper")?;
    c.insert("b", "Answers")?;
    c.insert("c", "There's no one")?;
    c.insert("d", "Out there")?; */

    assert_eq!(3, keyspace.partition_count());

    assert_eq!(1, a.tree.first_level_segment_count());
    assert_eq!(8, a.len()?);

    assert_eq!(1, b.tree.first_level_segment_count());
    assert_eq!(4, b.len()?);

    assert_eq!(0, c.tree.first_level_segment_count());
    assert_eq!(4, c.len()?);

    // TODO: call Keyspace::verify
    // needs to call Tree::verify on every partition and verify *all* journals

    Ok(())
}

#[test]
fn keyspace_load_v1_corrupt_journal() -> fjall::Result<()> {
    let folder = "test_fixture/v1_keyspace_corrupt_journal";

    let result = Config::new(folder).open();

    matches!(
        result,
        Err(fjall::Error::JournalRecovery(
            fjall::RecoveryError::CrcCheck
        ))
    );

    Ok(())
}
