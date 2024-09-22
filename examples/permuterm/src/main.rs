use fjall::{Config, Result};

fn permuterm(term: &str) -> Vec<String> {
    let mut v = vec![];

    let term_len = term.len();

    // We need to add $ to the end of the term
    let mut term = term.to_string();
    term.push('$');

    // Now we rotate the $ through the term
    v.push(term.clone());

    for _ in 0..term_len {
        let head = &term[0..1];
        let rest = &term[1..];
        term = format!("{rest}{head}");
        v.push(term.clone());
    }

    v
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

    println!("-- Wildcard query --");
    {
        for query in ["ter", "fos"] {
            // Permuterm wildcard queries are performed using: TERM*
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
