use fjall::{Config, Result};

struct Permutermifier {
    term: String,
    count: usize,
    len: usize,
}

impl Permutermifier {
    pub fn new(term: &str) -> Self {
        Self {
            term: format!("{term}$"),
            len: term.len(),
            count: term.len() + 1,
        }
    }

    fn rotate(&mut self) {
        let head = &self.term[0..1];
        let rest = &self.term[1..];
        self.term = format!("{rest}{head}");
    }
}

impl Iterator for Permutermifier {
    type Item = String;

    fn next(&mut self) -> Option<Self::Item> {
        if self.count == 0 {
            return None;
        }

        self.count -= 1;

        if self.count < self.len {
            self.rotate();
        }

        Some(self.term.clone())
    }
}

fn permuterm(term: &str) -> impl Iterator<Item = String> {
    Permutermifier::new(term)
}

const WORDS: &[&str] = &[
    "daughter",
    "laughter",
    "water",
    "blackwater",
    "waterway",
    "waterside",
    "waterfall",
    "waterfront",
    "arbiter",
    "test",
    "something",
    "mississippi",
    "watak",
    "teriyaki",
    "systrafoss",
    "skogafoss",
    "skiptarfoss",
    "seljalandsfoss",
    "gullfoss",
    "svartifoss",
    "hengifoss",
    "fagrifoss",
    "haifoss",
    "fossa",
    "foster",
    "fossil",
];

fn main() -> Result<()> {
    let keyspace = Config::default().temporary(true).open()?;

    let db = keyspace.open_partition("db", Default::default())?;

    for word in WORDS {
        for term in permuterm(word) {
            db.insert(format!("{term}#{word}"), word).unwrap();
        }
    }

    println!("-- Suffix query --");
    {
        let query = "ter";

        // Permuterm suffix queries are performed using: TERM$*
        for kv in db.prefix(format!("{query}$")) {
            let (_, v) = kv?;
            let v = std::str::from_utf8(&v).unwrap();
            println!("*{query} => {v:?}");
        }
    }
    println!();

    println!("-- Exact query --");
    {
        for query in ["water", "blackwater"] {
            // Permuterm exact queries are performed using: TERM$
            for kv in db.prefix(format!("{query}$#")) {
                let (_, v) = kv?;
                let v = std::str::from_utf8(&v).unwrap();
                println!("{query:?} => {v:?}");
            }
            println!();
        }
    }

    println!("-- Prefix query --");
    {
        let query = "water";

        // Permuterm suffix queries are performed using: $TERM*
        for kv in db.prefix(format!("${query}")) {
            let (_, v) = kv?;
            let v = std::str::from_utf8(&v).unwrap();
            println!("{query}* => {v:?}");
        }
    }
    println!();

    println!("-- Partial query --");
    {
        for query in ["ter", "fos"] {
            // Permuterm partial queries are performed using: TERM*
            for kv in db.prefix(format!("{query}")) {
                let (_, v) = kv?;
                let v = std::str::from_utf8(&v).unwrap();
                println!("*{query}* => {v:?}");
            }
            println!();
        }
    }

    println!("-- Between query --");
    {
        for (q1, q2) in [("s", "foss"), ("h", "foss")] {
            // Permuterm between queries are performed using: B$A*
            for kv in db.prefix(format!("{q2}${q1}")) {
                let (_, v) = kv?;
                let v = std::str::from_utf8(&v).unwrap();
                println!("{q1}*{q2} => {v:?}");
            }
            println!();
        }
    }

    Ok(())
}
