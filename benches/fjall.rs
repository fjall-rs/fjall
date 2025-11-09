use criterion::{criterion_group, criterion_main, Criterion};
use fjall::Database;

fn batch_write(c: &mut Criterion) {
    let dir = tempfile::tempdir().unwrap();

    let db = Database::builder(&dir).open().unwrap();

    let items = db.keyspace("default", Default::default()).unwrap();

    c.bench_function("Batch commit", |b| {
        b.iter(|| {
            let mut batch = db.batch();
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
