//! Generate an oversized active journal, then reopen it in a separate process.
//! Intended for cgroup memory-limit measurements of recovery, not normal use.

use fjall::{Database, KeyspaceCreateOptions};

fn main() -> fjall::Result<()> {
    let mut args = std::env::args().skip(1);
    let phase = args.next().expect("phase: seed, seed-batch, or reopen");
    let path = args.next().expect("database path");
    let rows: u64 = args
        .next()
        .expect("row count")
        .parse()
        .expect("valid row count");
    let value = [b'x'; 256];

    match phase.as_str() {
        "seed" => {
            let db = Database::builder(&path)
                .worker_threads_unchecked(0)
                .open()?;
            let keyspace = db.keyspace("oversized", || {
                KeyspaceCreateOptions::default().max_memtable_size(8 * 1024 * 1024)
            })?;
            for i in 0..rows {
                keyspace.insert(i.to_be_bytes(), value)?;
            }
            println!("seeded rows={rows} write_buffer={}", db.write_buffer_size());
        }
        "seed-batch" => {
            let db = Database::builder(&path)
                .worker_threads_unchecked(0)
                .open()?;
            let keyspace = db.keyspace("oversized", || {
                KeyspaceCreateOptions::default().max_memtable_size(8 * 1024 * 1024)
            })?;
            let mut batch = db.batch();
            batch.insert(&keyspace, "duplicate", "old");
            for i in 0..rows {
                batch.insert(&keyspace, i.to_be_bytes(), value);
            }
            batch.insert(&keyspace, "duplicate", "new");
            batch.commit()?;
            println!(
                "seeded one batch rows={rows} write_buffer={}",
                db.write_buffer_size()
            );
        }
        "reopen" => {
            eprintln!("opening database");
            let workers = std::env::var("FJALL_PROBE_WORKERS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(1);
            let db = Database::builder(&path)
                .worker_threads_unchecked(workers)
                .open()?;
            eprintln!("database open complete");
            let keyspace = db.keyspace("oversized", KeyspaceCreateOptions::default)?;
            for i in [0, rows / 2, rows - 1] {
                eprintln!("reading key {i}");
                assert_eq!(
                    keyspace.get(i.to_be_bytes())?.as_deref(),
                    Some(value.as_slice())
                );
            }
            if keyspace.get("duplicate")?.is_some() {
                assert_eq!(
                    keyspace.get("duplicate")?.as_deref(),
                    Some(b"new".as_slice())
                );
            }
            let write_buffer = db.write_buffer_size();
            drop(keyspace);
            drop(db);
            let status = std::fs::read_to_string("/proc/self/status")?;
            let high_water = status
                .lines()
                .find(|line| line.starts_with("VmHWM:"))
                .unwrap_or("VmHWM unavailable");
            let cgroup = std::fs::read_to_string("/proc/self/cgroup")?;
            let cgroup_path = cgroup
                .lines()
                .find_map(|line| line.strip_prefix("0::"))
                .unwrap_or("/");
            let peak_path = format!("/sys/fs/cgroup{cgroup_path}/memory.peak");
            let cgroup_peak = std::fs::read_to_string(peak_path)
                .map(|value| value.trim().to_owned())
                .unwrap_or_else(|_| "unavailable".to_owned());
            println!(
                "recovered rows={rows} write_buffer={} {high_water} cgroup_peak={cgroup_peak}",
                write_buffer,
            );
        }
        _ => panic!("phase must be seed, seed-batch, or reopen"),
    }
    Ok(())
}
