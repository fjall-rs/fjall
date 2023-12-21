use lsm_tree::{MemTable, SeqNo, Value, ValueType};
use serde::{Deserialize, Serialize};
use std::io::{Seek, Write};
use std::sync::Mutex;
use std::{
    fs::{File, OpenOptions},
    io::{BufRead, BufReader},
    path::Path,
    sync::Arc,
};

#[derive(Deserialize, Serialize)]
pub struct WalEntry {
    #[serde(rename = "k")]
    key: Arc<str>,

    #[serde(rename = "v")]
    value: Arc<str>,

    #[serde(rename = "s")]
    seqno: SeqNo,

    #[serde(rename = "t")]
    value_type: u8,
}

impl From<WalEntry> for Value {
    fn from(entry: WalEntry) -> Self {
        Self {
            key: entry.key.into(),
            value: entry.value.into(),
            seqno: entry.seqno,
            value_type: ValueType::from(entry.value_type),
        }
    }
}

impl From<Value> for WalEntry {
    fn from(entry: Value) -> Self {
        Self {
            key: std::str::from_utf8(&entry.key)
                .expect("should be valid utf-8")
                .into(),
            value: std::str::from_utf8(&entry.value)
                .expect("should be valid utf-8")
                .into(),
            seqno: entry.seqno,
            value_type: entry.value_type.into(),
        }
    }
}

/// Simple JSON-based single-writer-only WAL.
#[derive(Clone)]
pub struct Wal {
    writer: Arc<Mutex<File>>,
}

impl Wal {
    pub fn open<P: AsRef<Path>>(path: P) -> lsm_tree::Result<(Wal, MemTable)> {
        let path = path.as_ref();
        let wal_path = path.join(".wal.jsonl");

        if wal_path.try_exists()? {
            let memtable = recover_wal(&wal_path)?;
            let writer = OpenOptions::new().append(true).open(&wal_path)?;
            let writer = Arc::new(Mutex::new(writer));

            let wal = Self { writer };

            Ok((wal, memtable))
        } else {
            let writer = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&wal_path)?;
            let writer = Arc::new(Mutex::new(writer));

            let wal = Self { writer };
            Ok((wal, MemTable::default()))
        }
    }

    pub fn write(&mut self, value: Value) -> lsm_tree::Result<()> {
        let mut writer = self.writer.lock().expect("lock is poisoned");

        let wal_entry: WalEntry = value.into();
        let str = serde_json::to_string(&wal_entry).expect("should serialize");
        writeln!(&mut writer, "{str}")?;

        Ok(())
    }

    pub fn sync(&self) -> lsm_tree::Result<()> {
        let writer = self.writer.lock().expect("lock is poisoned");
        writer.sync_all()?;
        Ok(())
    }

    pub fn truncate(&mut self) -> lsm_tree::Result<()> {
        let mut writer = self.writer.lock().expect("lock is poisoned");
        writer.seek(std::io::SeekFrom::Start(0))?;
        writer.set_len(0)?;
        writer.sync_all()?;
        Ok(())
    }
}

fn recover_wal<P: AsRef<Path>>(path: P) -> lsm_tree::Result<MemTable> {
    eprintln!("Recovering WAL");

    let memtable = MemTable::default();

    let wal_path = path.as_ref();
    let file = File::open(wal_path)?;
    let file = BufReader::new(file);

    let mut cnt = 0;

    for (idx, line) in file.lines().enumerate() {
        let line = line?;
        if line.is_empty() {
            break;
        }

        let Ok(entry) = serde_json::from_str::<WalEntry>(&line) else {
            eprintln!("Truncating WAL to line {idx} because of malformed content");
            break;
        };

        memtable.insert(entry.into());
        cnt += 1;
    }

    eprintln!("Recovered {cnt} items from WAL");

    Ok(memtable)
}
