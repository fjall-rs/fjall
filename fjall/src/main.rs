use fjall::{Config, PartitionConfig};
use std::time::Instant;
use std::time::{SystemTime, UNIX_EPOCH};

fn generate_random_string() -> String {
    // Seed the generator using the current timestamp
    let seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_nanos() as u64;

    let mut rng = Lcg::new(seed);

    // Define the characters you want in your random string
    let charset = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

    // Generate the random string
    let random_string: String = (0..256)
        .map(|_| {
            let index = rng.next_u64() as usize % charset.len();
            charset.chars().nth(index).unwrap()
        })
        .collect();

    random_string
}

// Linear Congruential Generator (LCG)
struct Lcg {
    state: u64,
}

impl Lcg {
    fn new(seed: u64) -> Self {
        Lcg { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        const A: u64 = 6364136223846793005;
        const C: u64 = 1442695040888963407;
        self.state = A.wrapping_mul(self.state).wrapping_add(C);
        self.state
    }
}

pub fn main() -> fjall::Result<()> {
    env_logger::init();

    eprintln!("hello");

    let keyspace = Config::new(".data").open()?;

    /* for x in 0..10 {
        keyspace.open_partition(&format!("data-{x}"), PartitionConfig::default())?;
        eprintln!("partition {x} opened");
    } */

    let items = keyspace.open_partition("default", PartitionConfig::default())?;

    for x in 0u64..5_000_000 {
        items.insert(
            x.to_be_bytes(),
            "321awwararwabrwarwarbyxyccxyxcyxycycxyxcxc",
        )?;
    }

    keyspace.persist()?;

    eprintln!("hello partitions");

    // loop {}

    /*  let start = Instant::now();

    for x in 0..100 {
        items.insert(format!("hello world-{x}"), generate_random_string())?;
        keyspace.persist()?;
    }

    eprintln!("Took {}s", start.elapsed().as_secs_f32());

    assert_eq!(1000, items.len()?); */

    /* for x in 0..50_000_000 {
        assert!(items.get(format!("hello world-{x}"))?.is_some());
    } */

    /*  assert!(items.get("hello world-0")?.is_some());
    assert!(items.get("hello world-1")?.is_some());
    assert!(items.get("hello world-2")?.is_some());
    assert!(items.get("hello world-3")?.is_some());
    assert!(items.get("hello world-4")?.is_some());
    assert!(items.get("hello world-5")?.is_some());
    assert!(items.get("hello world-6")?.is_some());
    assert!(items.get("hello world-7")?.is_some());
    assert!(items.get("hello world-8")?.is_some());
    assert!(items.get("hello world-9")?.is_some());

    items
        .tree
        .flush_active_memtable()
        .unwrap()
        .join()
        .unwrap()?;

    assert!(items.get("hello world-0")?.is_some());
    assert!(items.get("hello world-1")?.is_some());
    assert!(items.get("hello world-2")?.is_some());
    assert!(items.get("hello world-3")?.is_some());
    assert!(items.get("hello world-4")?.is_some());
    assert!(items.get("hello world-5")?.is_some());
    assert!(items.get("hello world-6")?.is_some());
    assert!(items.get("hello world-7")?.is_some());
    assert!(items.get("hello world-8")?.is_some());
    assert!(items.get("hello world-9")?.is_some());

    items.remove("hello world-0")?;
    items.remove("hello world-1")?;
    items.remove("hello world-2")?;
    items.remove("hello world-3")?;
    items.remove("hello world-4")?;
    items.remove("hello world-5")?;
    items.remove("hello world-6")?;
    items.remove("hello world-7")?;
    items.remove("hello world-8")?;
    items.remove("hello world-9")?;

    assert!(items.get("hello world-0")?.is_none());
    assert!(items.get("hello world-1")?.is_none());
    assert!(items.get("hello world-2")?.is_none());
    assert!(items.get("hello world-3")?.is_none());
    assert!(items.get("hello world-4")?.is_none());
    assert!(items.get("hello world-5")?.is_none());
    assert!(items.get("hello world-6")?.is_none());
    assert!(items.get("hello world-7")?.is_none());
    assert!(items.get("hello world-8")?.is_none());
    assert!(items.get("hello world-9")?.is_none());

    items
        .tree
        .flush_active_memtable()
        .unwrap()
        .join()
        .unwrap()?;

    assert!(items.get("hello world-0")?.is_none());
    assert!(items.get("hello world-1")?.is_none());
    assert!(items.get("hello world-2")?.is_none());
    assert!(items.get("hello world-3")?.is_none());
    assert!(items.get("hello world-4")?.is_none());
    assert!(items.get("hello world-5")?.is_none());
    assert!(items.get("hello world-6")?.is_none());
    assert!(items.get("hello world-7")?.is_none());
    assert!(items.get("hello world-8")?.is_none());
    assert!(items.get("hello world-9")?.is_none()); */

    drop(keyspace);
    println!("I'm doing OK");

    Ok(())
}
