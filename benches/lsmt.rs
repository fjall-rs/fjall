use criterion::{criterion_group, criterion_main, Criterion};
use lsm_tree::{BlockCache, Config};
use rand::Rng;
use tempfile::tempdir;

fn insert(c: &mut Criterion) {
    let item_count = 100_000;

    let mut group = c.benchmark_group("inserts");
    group.sample_size(10);
    group.throughput(criterion::Throughput::Elements(item_count as u64));

    for thread_count in [1_u32, 2, 4 /* , 8*/] {
        group.bench_function(
            format!("{} inserts ({} threads)", item_count, thread_count),
            |b| {
                let tree = Config::new(tempdir().unwrap()).open().unwrap();

                b.iter(|| {
                    let mut threads = vec![];

                    for _ in 0..thread_count {
                        let tree = tree.clone();

                        threads.push(std::thread::spawn(move || {
                            for _ in 0..(item_count / thread_count) {
                                let key = nanoid::nanoid!();
                                let value = nanoid::nanoid!();
                                tree.insert(key, value).unwrap();
                            }
                        }));
                    }

                    for thread in threads {
                        thread.join().unwrap();
                    }
                })
            },
        );
    }
}

fn memtable_point_reads(c: &mut Criterion) {
    let mut group = c.benchmark_group("memtable point reads");
    group.sample_size(10);

    let tree = Config::new(tempdir().unwrap())
        .max_memtable_size(128_000_000)
        .open()
        .unwrap();

    let max = 1_000_000;
    let lookup_count = 1_000_000;

    group.throughput(criterion::Throughput::Elements(lookup_count as u64));

    for x in 0_u32..max {
        let key = x.to_be_bytes();
        let value = nanoid::nanoid!();
        tree.insert(key, value).unwrap();
    }

    assert_eq!(tree.len().unwrap() as u32, max);
    assert_eq!(0, tree.segment_count());

    for thread_count in [1_u32, 2, 4 /* , 8*/] {
        group.bench_function(
            format!("{} point reads ({} threads)", lookup_count, thread_count),
            |b| {
                b.iter(|| {
                    let mut threads = vec![];

                    for _ in 0..thread_count {
                        let tree = tree.clone();

                        threads.push(std::thread::spawn(move || {
                            let mut rng = rand::thread_rng();

                            for _ in 0_u32..(lookup_count / thread_count) {
                                let key = rng.gen_range(0..max);
                                assert!(tree.get(key.to_be_bytes()).unwrap().is_some());
                            }
                        }));
                    }

                    for thread in threads {
                        thread.join().unwrap();
                    }
                })
            },
        );
    }
}

fn disk_point_reads(c: &mut Criterion) {
    let mut group = c.benchmark_group("disk point reads");
    group.sample_size(10);

    let tree = Config::new(tempdir().unwrap())
        .block_cache(BlockCache::with_capacity_blocks(0).into())
        .open()
        .unwrap();

    let max = 1_000_000;
    let lookup_count = 100_000;

    group.throughput(criterion::Throughput::Elements(lookup_count as u64));

    for x in 0_u32..max {
        let key = x.to_be_bytes();
        let value = nanoid::nanoid!();
        tree.insert(key, value).unwrap();
    }

    tree.wait_for_memtable_flush().expect("Flush thread failed");
    assert_eq!(tree.len().unwrap() as u32, max);

    for thread_count in [1_u32, 2, 4 /* , 8*/] {
        group.bench_function(
            format!("{} point reads ({} threads)", lookup_count, thread_count),
            |b| {
                b.iter(|| {
                    let mut threads = vec![];

                    for _ in 0..thread_count {
                        let tree = tree.clone();

                        threads.push(std::thread::spawn(move || {
                            let mut rng = rand::thread_rng();

                            for _ in 0_u32..(lookup_count / thread_count) {
                                let key = rng.gen_range(0..max);
                                assert!(tree.get(key.to_be_bytes()).unwrap().is_some());
                            }
                        }));
                    }

                    for thread in threads {
                        thread.join().unwrap();
                    }
                })
            },
        );

        group.bench_function(
            format!(
                "{} snapshot point reads ({} threads)",
                lookup_count, thread_count
            ),
            |b| {
                b.iter(|| {
                    let mut threads = vec![];

                    for _ in 0..thread_count {
                        let snapshot = tree.snapshot();

                        threads.push(std::thread::spawn(move || {
                            let mut rng = rand::thread_rng();

                            for _ in 0_u32..(lookup_count / thread_count) {
                                let key = rng.gen_range(0..max);
                                assert!(snapshot.get(key.to_be_bytes()).unwrap().is_some());
                            }
                        }));
                    }

                    for thread in threads {
                        thread.join().unwrap();
                    }
                })
            },
        );
    }
}

fn cached_retrieve_disk_random(c: &mut Criterion) {
    let mut group = c.benchmark_group("disk point reads (with cache)");
    group.sample_size(10);

    let tree = Config::new(tempdir().unwrap())
        .block_cache(BlockCache::with_capacity_blocks(/* 256 MB */ 62 * 1_000).into())
        .open()
        .unwrap();

    let max = 1_000_000;
    let lookup_count = 100_000;

    group.throughput(criterion::Throughput::Elements(lookup_count as u64));

    for x in 0_u32..max {
        let key = x.to_be_bytes();
        let value = nanoid::nanoid!();
        tree.insert(key, value).unwrap();
    }

    tree.wait_for_memtable_flush().expect("Flush thread failed");
    assert_eq!(tree.len().unwrap() as u32, max);

    for thread_count in [1_u32, 2, 4 /* , 8*/] {
        group.bench_function(
            format!("{} point reads ({} threads)", lookup_count, thread_count),
            |b| {
                b.iter(|| {
                    let mut threads = vec![];

                    for _ in 0..thread_count {
                        let tree = tree.clone();

                        threads.push(std::thread::spawn(move || {
                            let mut rng = rand::thread_rng();

                            for _ in 0_u32..(lookup_count / thread_count) {
                                let key = rng.gen_range(0..max);
                                assert!(tree.get(key.to_be_bytes()).unwrap().is_some());
                            }
                        }));
                    }

                    for thread in threads {
                        thread.join().unwrap();
                    }
                })
            },
        );

        group.bench_function(
            format!(
                "{} snapshot point reads ({} threads)",
                lookup_count, thread_count
            ),
            |b| {
                b.iter(|| {
                    let mut threads = vec![];

                    for _ in 0..thread_count {
                        let snapshot = tree.snapshot();

                        threads.push(std::thread::spawn(move || {
                            let mut rng = rand::thread_rng();

                            for _ in 0_u32..(lookup_count / thread_count) {
                                let key = rng.gen_range(0..max);
                                assert!(snapshot.get(key.to_be_bytes()).unwrap().is_some());
                            }
                        }));
                    }

                    for thread in threads {
                        thread.join().unwrap();
                    }
                })
            },
        );
    }
}

fn full_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("full scan");
    group.sample_size(10);

    let item_count = 500_000;

    group.bench_function("full scan uncached", |b| {
        let tree = Config::new(tempdir().unwrap())
            .block_cache(BlockCache::with_capacity_blocks(0).into())
            .open()
            .unwrap();

        for x in 0_u32..item_count {
            let key = x.to_be_bytes();
            let value = nanoid::nanoid!();
            tree.insert(key, value).expect("Insert error");
        }

        tree.force_memtable_flush()
            .expect("Flush error")
            .join()
            .expect("Join failed")
            .expect("Flush thread failed");

        b.iter(|| {
            assert_eq!(tree.len().unwrap(), item_count as usize);
        })
    });

    group.bench_function("full scan cached", |b| {
        let tree = Config::new(tempdir().unwrap()).open().unwrap();

        for x in 0_u32..item_count {
            let key = x.to_be_bytes();
            let value = nanoid::nanoid!();
            tree.insert(key, value).expect("Insert error");
        }

        tree.force_memtable_flush()
            .expect("Flush error")
            .join()
            .expect("Join failed")
            .expect("Flush thread failed");
        assert_eq!(tree.len().unwrap(), item_count as usize);

        b.iter(|| {
            assert_eq!(tree.len().unwrap(), item_count as usize);
        })
    });
}

fn scan_vs_query(c: &mut Criterion) {
    use std::ops::Bound::*;

    let mut group = c.benchmark_group("scan vs query");

    for size in [100_000, 1_000_000, 2_000_000] {
        let tree = Config::new(tempdir().unwrap()).open().unwrap();

        for x in 0..size as u64 {
            let key = x.to_be_bytes().to_vec();
            let value = nanoid::nanoid!().as_bytes().to_vec();
            tree.insert(key, value).expect("Insert error");
        }

        tree.wait_for_memtable_flush().expect("Flush thread failed");
        assert_eq!(tree.len().unwrap(), size);

        group.sample_size(10);
        group.bench_function(format!("scan {}", size), |b| {
            b.iter(|| {
                let iter = tree.iter().unwrap();
                let iter = iter.into_iter();
                let count = iter
                    .filter(|x| match x {
                        Ok((key, _)) => {
                            let buf = &key[..8];
                            let (int_bytes, _rest) = buf.split_at(std::mem::size_of::<u64>());
                            let num = u64::from_be_bytes(int_bytes.try_into().unwrap());
                            (60000..61000).contains(&num)
                        }
                        Err(_) => false,
                    })
                    .count();
                assert_eq!(count, 1000);
            })
        });
        group.bench_function(format!("query {}", size), |b| {
            b.iter(|| {
                let iter = tree
                    .range((
                        Included(60000_u64.to_be_bytes().to_vec()),
                        Excluded(61000_u64.to_be_bytes().to_vec()),
                    ))
                    .unwrap();
                let iter = iter.into_iter();
                assert_eq!(iter.count(), 1000);
            })
        });
        group.bench_function(format!("query rev {}", size), |b| {
            b.iter(|| {
                let iter = tree
                    .range((
                        Included(60000_u64.to_be_bytes().to_vec()),
                        Excluded(61000_u64.to_be_bytes().to_vec()),
                    ))
                    .unwrap();
                let iter = iter.into_iter();
                assert_eq!(iter.rev().count(), 1000);
            })
        });
    }
}

fn scan_vs_prefix(c: &mut Criterion) {
    let mut group = c.benchmark_group("scan vs prefix");

    for size in [100_000_u64, 1_000_000, 2_000_000] {
        let tree = Config::new(tempdir().unwrap()).open().unwrap();

        for _ in 0..size {
            let key = nanoid::nanoid!();
            let value = nanoid::nanoid!();
            tree.insert(key, value).expect("Insert error");
        }

        let prefix = "hello$$$";

        for _ in 0..1000_u64 {
            let key = format!("{}:{}", prefix, nanoid::nanoid!());
            let value = nanoid::nanoid!();
            tree.insert(key, value).expect("Insert error");
        }

        tree.wait_for_memtable_flush().expect("Flush thread failed");
        assert_eq!(tree.len().unwrap() as u64, size + 1000);

        group.sample_size(10);
        group.bench_function(format!("scan {}", size), |b| {
            b.iter(|| {
                let iter = tree.iter().unwrap();
                let iter = iter.into_iter().filter(|x| match x {
                    Ok((key, _)) => key.starts_with(prefix.as_bytes()),
                    Err(_) => false,
                });
                assert_eq!(iter.count(), 1000);
            });
        });
        group.bench_function(format!("prefix {}", size), |b| {
            b.iter(|| {
                let iter = tree.prefix(prefix).unwrap();
                let iter = iter.into_iter();
                assert_eq!(iter.count(), 1000);
            });
        });
        group.bench_function(format!("prefix rev {}", size), |b| {
            b.iter(|| {
                let iter = tree.prefix(prefix).unwrap();
                let iter = iter.into_iter();
                assert_eq!(iter.rev().count(), 1000);
            });
        });
    }
}

fn recover_tree(c: &mut Criterion) {
    let mut group = c.benchmark_group("recover tree");

    for item_count in [100, 1_000, 10_000, 100_000, 1_000_000, 2_000_000] {
        let tree = Config::new(tempdir().unwrap()).open().unwrap();

        for _ in 0..item_count {
            let key = nanoid::nanoid!();
            let value = nanoid::nanoid!();
            tree.insert(key, value).expect("Insert error");
        }

        tree.wait_for_memtable_flush().expect("should flush");
        drop(tree);

        group.bench_function(format!("recover {} items", item_count), |b| {
            b.iter(|| {
                let _tree = Config::new(tempdir().unwrap()).open().unwrap();
            })
        });
    }
}

criterion_group!(
    benches,
    /* insert,
    memtable_point_reads, */
    recover_tree,
    /*  disk_point_reads,
    cached_retrieve_disk_random,
    full_scan,
    scan_vs_query,
    scan_vs_prefix, */
);

criterion_main!(benches);
