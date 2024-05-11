use fjall::{Config, Keyspace, PartitionHandle};
use serde_json::Value;
use std::path::Path;

struct Triplestore {
    #[allow(dead_code)]
    keyspace: Keyspace,

    subjects: PartitionHandle,
    verbs: PartitionHandle,
}

impl Triplestore {
    pub fn new<P: AsRef<Path>>(path: P) -> fjall::Result<Self> {
        let keyspace = Config::new(path).open()?;
        let subjects = keyspace.open_partition("subjects", Default::default())?;
        let verbs = keyspace.open_partition("verbs", Default::default())?;

        Ok(Self {
            keyspace,
            subjects,
            verbs,
        })
    }

    pub fn add_subject(&self, key: &str, data: &Value) -> fjall::Result<()> {
        self.subjects
            .insert(key, serde_json::to_string(data).expect("should serialize"))
    }

    pub fn add_triple(&self, from: &str, verb: &str, to: &str, data: &Value) -> fjall::Result<()> {
        self.verbs.insert(
            format!("{from}#{verb}#{to}"),
            serde_json::to_string(data).expect("should serialize"),
        )
    }

    pub fn contains_subject(&self, key: &str) -> fjall::Result<bool> {
        self.subjects.contains_key(key)
    }

    pub fn get_triple(
        &self,
        subject: &str,
        verb: &str,
        object: &str,
    ) -> fjall::Result<Option<Value>> {
        let Some(bytes) = self.verbs.get(format!("{subject}#{verb}#{object}"))? else {
            return Ok(None);
        };
        let value = std::str::from_utf8(&bytes).expect("should be utf-8");
        let value = serde_json::from_str(value).expect("should be json");
        Ok(Some(value))
    }

    pub fn out(
        &self,
        subject: &str,
        verb: &str,
    ) -> fjall::Result<Vec<(String, String, String, Value)>> {
        let mut result = vec![];

        for item in self.verbs.prefix(format!("{subject}#{verb}#")).into_iter() {
            let (key, value) = item?;

            let key = std::str::from_utf8(&key).expect("should be utf-8");
            let mut splits = key.split('#');
            let s = splits.next().unwrap().to_string();
            let v = splits.next().unwrap().to_string();
            let o = splits.next().unwrap().to_string();

            let value = std::str::from_utf8(&value).expect("should be utf-8");
            let value: Value = serde_json::from_str(&value).expect("should be json");

            result.push((s, v, o, value));
        }

        Ok(result)
    }
}

fn main() -> fjall::Result<()> {
    let store = Triplestore::new(".data")?;

    if !store.contains_subject("person-1")? {
        store.add_subject(
            "person-1",
            &serde_json::json!({
                "name": "Peter"
            }),
        )?;
    }
    if !store.contains_subject("person-2")? {
        store.add_subject(
            "person-2",
            &serde_json::json!({
                "name": "Peter 2"
            }),
        )?;
    }
    if !store.contains_subject("person-3")? {
        store.add_subject(
            "person-3",
            &serde_json::json!({
                "name": "Peter 3"
            }),
        )?;
    }
    if !store.contains_subject("person-4")? {
        store.add_subject(
            "person-4",
            &serde_json::json!({
                "name": "Peter 4"
            }),
        )?;
    }

    for person in &["person-2", "person-4"] {
        if (store.get_triple("person-1", "knows", person)?).is_none() {
            store.add_triple(
                "person-1",
                "knows",
                person,
                &serde_json::json!({
                    "since": 2014
                }),
            )?;
        }
    }

    eprintln!("Listing all person-1->knows-> relations:");
    for (_, _, o, _) in store.out("person-1", "knows")? {
        eprintln!("person-1 knows {o}!");
    }

    Ok(())
}
