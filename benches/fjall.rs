use criterion::{criterion_group, criterion_main, Criterion};

fn batch_write(c: &mut Criterion) {
    let dir = tempfile::tempdir().unwrap();

    let keyspace = fjall::Config::new(&dir).open().unwrap();
    let items = keyspace
        .open_partition("default", Default::default())
        .unwrap();

    c.bench_function("Batch commit", |b| {
        b.iter(|| {
            let mut batch = keyspace.batch();
            for item in 'a'..='z' {
                let item = item.to_string();
                batch.insert(&items, &item, &item);
            }
            batch.commit().unwrap();
        });
    });
}

criterion_group!(benches, batch_write);
criterion_main!(benches);
