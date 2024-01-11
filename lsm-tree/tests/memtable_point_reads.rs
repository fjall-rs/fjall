use lsm_tree::{Value, ValueType};
use test_log::test;

#[test]
fn memtable_mvcc_point_read() -> lsm_tree::Result<()> {
    let memtable = lsm_tree::MemTable::default();

    memtable.insert(Value {
        key: "hello-key-999991".as_bytes().into(),
        value: "hello-value-999991".as_bytes().into(),
        seqno: 0,
        value_type: ValueType::Value,
    });

    let item = memtable.get("hello-key-99999".as_bytes(), None);
    assert_eq!(None, item);

    let item = memtable.get("hello-key-999991".as_bytes(), None);
    assert_eq!("hello-value-999991".as_bytes(), &*item.unwrap().value);

    memtable.insert(Value {
        key: "hello-key-999991".as_bytes().into(),
        value: "hello-value-999991-2".as_bytes().into(),
        seqno: 1,
        value_type: ValueType::Value,
    });

    let item = memtable.get("hello-key-99999".as_bytes(), None);
    assert_eq!(None, item);

    let item = memtable.get("hello-key-999991".as_bytes(), None);
    assert_eq!("hello-value-999991-2".as_bytes(), &*item.unwrap().value);

    let item = memtable.get("hello-key-99999".as_bytes(), Some(1));
    assert_eq!(None, item);

    let item = memtable.get("hello-key-999991".as_bytes(), Some(1));
    assert_eq!("hello-value-999991".as_bytes(), &*item.unwrap().value);

    let item = memtable.get("hello-key-99999".as_bytes(), Some(2));
    assert_eq!(None, item);

    let item = memtable.get("hello-key-999991".as_bytes(), Some(2));
    assert_eq!("hello-value-999991-2".as_bytes(), &*item.unwrap().value);

    Ok(())
}
