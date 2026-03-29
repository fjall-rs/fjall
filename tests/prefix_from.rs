use fjall::{Database, KeyspaceCreateOptions, Readable};

fn setup() -> (tempfile::TempDir, Database, fjall::Keyspace) {
    let folder = tempfile::tempdir().unwrap();
    let db = Database::builder(&folder).open().unwrap();
    let tree = db
        .keyspace("default", KeyspaceCreateOptions::default)
        .unwrap();
    (folder, db, tree)
}

#[test]
fn prefix_from_happy_path() {
    let (_dir, _db, tree) = setup();
    tree.insert("ab:1", "v1").unwrap();
    tree.insert("ab:2", "v2").unwrap();
    tree.insert("ab:3", "v3").unwrap();
    assert_eq!(2, tree.prefix_from("ab", "ab:2").count());
}

#[test]
fn prefix_from_all_keys() {
    let (_dir, _db, tree) = setup();
    tree.insert("ab:1", "v1").unwrap();
    tree.insert("ab:2", "v2").unwrap();
    tree.insert("ab:3", "v3").unwrap();
    // from == prefix should return all prefix keys
    assert_eq!(3, tree.prefix_from("ab", "ab").count());
}

#[test]
fn prefix_from_clamps_when_from_before_prefix() {
    let (_dir, _db, tree) = setup();
    tree.insert("aa:1", "v0").unwrap();
    tree.insert("ab:1", "v1").unwrap();
    tree.insert("ab:2", "v2").unwrap();
    tree.insert("ac:1", "v3").unwrap();

    // from="aa:1" is before prefix="ab", should clamp to "ab" and never yield "aa:1"
    let keys: Vec<_> = tree
        .prefix_from("ab", "aa:1")
        .map(|g| {
            let k = g.key().unwrap();
            String::from_utf8(k.to_vec()).unwrap()
        })
        .collect();

    assert_eq!(keys, vec!["ab:1", "ab:2"]);
}

#[test]
fn prefix_from_empty_when_from_beyond_range() {
    let (_dir, _db, tree) = setup();
    tree.insert("ab:1", "v1").unwrap();
    tree.insert("ab:2", "v2").unwrap();

    // "zz" is far beyond the "ab" prefix range upper bound ("ac")
    assert_eq!(0, tree.prefix_from("ab", "zz").count());
}

#[test]
fn prefix_from_with_non_prefix_from_in_range() {
    let (_dir, _db, tree) = setup();
    tree.insert("ab:1", "v1").unwrap();
    tree.insert("ab:5", "v2").unwrap();
    tree.insert("ab:9", "v3").unwrap();

    // from="ab:3" — doesn't match any key, but is within prefix range
    let keys: Vec<_> = tree
        .prefix_from("ab", "ab:3")
        .map(|g| {
            let k = g.key().unwrap();
            String::from_utf8(k.to_vec()).unwrap()
        })
        .collect();

    assert_eq!(keys, vec!["ab:5", "ab:9"]);
}

#[test]
fn prefix_from_all_0xff_prefix() {
    let (_dir, _db, tree) = setup();
    let prefix = [0xFF, 0xFF];
    let key1 = [0xFF, 0xFF, 0x01];
    let key2 = [0xFF, 0xFF, 0x02];
    let key3 = [0xFF, 0xFF, 0x03];
    tree.insert(key1, "v1").unwrap();
    tree.insert(key2, "v2").unwrap();
    tree.insert(key3, "v3").unwrap();

    // Start from key2 — should get key2 and key3
    assert_eq!(2, tree.prefix_from(prefix, key2).count());
}

/// Snapshot-based prefix_from via Readable trait.
#[test]
fn prefix_from_snapshot_isolation() {
    let (_dir, db, tree) = setup();
    tree.insert("ab:1", "v1").unwrap();
    tree.insert("ab:2", "v2").unwrap();
    tree.insert("ab:3", "v3").unwrap();

    let snapshot = db.snapshot();

    // Insert after snapshot
    tree.insert("ab:4", "v4").unwrap();

    // Snapshot should see only 2 items from ab:2 onward (not ab:4)
    assert_eq!(2, snapshot.prefix_from(&tree, "ab", "ab:2").count());
}

/// Empty prefix degenerates to a range scan from `from` to end of keyspace.
#[test]
fn prefix_from_empty_prefix() {
    let (_dir, _db, tree) = setup();
    tree.insert("a", "v1").unwrap();
    tree.insert("b", "v2").unwrap();
    tree.insert("c", "v3").unwrap();
    tree.insert("d", "v4").unwrap();

    // Empty prefix + from="b" → scan from "b" onward
    assert_eq!(3, tree.prefix_from("", "b").count());
}

/// Reverse iteration via next_back().
#[test]
fn prefix_from_reverse_iteration() {
    let (_dir, _db, tree) = setup();
    tree.insert("ab:1", "v1").unwrap();
    tree.insert("ab:2", "v2").unwrap();
    tree.insert("ab:3", "v3").unwrap();
    tree.insert("ab:4", "v4").unwrap();

    let mut iter = tree.prefix_from("ab", "ab:2");
    // Forward: first should be ab:2
    let first = iter.next().unwrap();
    assert_eq!(&*first.key().unwrap(), b"ab:2");
    // Backward: last remaining should be ab:4
    let last = iter.next_back().unwrap();
    assert_eq!(&*last.key().unwrap(), b"ab:4");
}

/// Empty keyspace returns empty iterator.
#[test]
fn prefix_from_empty_keyspace() {
    let (_dir, _db, tree) = setup();
    assert_eq!(0, tree.prefix_from("ab", "ab:1").count());
}

/// Snapshot-based clamp behavior via Readable trait.
#[test]
fn prefix_from_snapshot_clamps_when_from_before_prefix() {
    let (_dir, db, tree) = setup();
    tree.insert("aa:1", "v0").unwrap();
    tree.insert("ab:1", "v1").unwrap();
    tree.insert("ab:2", "v2").unwrap();

    let snapshot = db.snapshot();

    let keys: Vec<_> = snapshot
        .prefix_from(&tree, "ab", "aa:1")
        .map(|g| {
            let k = g.key().unwrap();
            String::from_utf8(k.to_vec()).unwrap()
        })
        .collect();

    assert_eq!(keys, vec!["ab:1", "ab:2"]);
}
