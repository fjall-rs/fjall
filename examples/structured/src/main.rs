use fjall::{Config, PartitionCreateOptions, PartitionHandle};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
struct Song {
    /// ID
    #[serde(skip)]
    id: String,

    /// Artist name
    artist: String,

    /// Title
    title: String,

    /// Release year
    release_year: u16,
}

impl std::fmt::Display for Song {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} - {} ({})",
            self.artist, self.title, self.release_year
        )
    }
}

impl Song {
    pub fn store(&self, tree: &PartitionHandle) -> fjall::Result<()> {
        let serialized = rmp_serde::to_vec(self).expect("should serialize");
        tree.insert(&self.id, serialized)?;
        Ok(())
    }

    pub fn load(tree: &PartitionHandle, key: &str) -> fjall::Result<Option<Song>> {
        let Some(item) = tree.get(key)? else {
            return Ok(None);
        };
        let mut item: Song = rmp_serde::from_slice(&item).expect("should deserialize");
        item.id = key.to_owned();
        Ok(Some(item))
    }
}

fn main() -> fjall::Result<()> {
    let items = vec![
        Song {
            id: "clairo:amoeba".to_owned(),
            release_year: 2021,
            artist: "Clairo".to_owned(),
            title: "Amoeba".to_owned(),
        },
        Song {
            id: "clairo:zinnias".to_owned(),
            release_year: 2021,
            artist: "Clairo".to_owned(),
            title: "Zinnias".to_owned(),
        },
        Song {
            id: "fazerdaze:break".to_owned(),
            release_year: 2022,
            artist: "Fazerdaze".to_owned(),
            title: "Break!".to_owned(),
        },
        Song {
            id: "fazerdaze:winter".to_owned(),
            release_year: 2022,
            artist: "Fazerdaze".to_owned(),
            title: "Winter".to_owned(),
        },
    ];

    let keyspace = Config::default().open()?;
    let db = keyspace.open_partition("songs", PartitionCreateOptions::default())?;

    for item_to_insert in items {
        if let Some(item) = Song::load(&db, &item_to_insert.id)? {
            eprintln!("Found: {item}");
            assert_eq!(item, item_to_insert);
        } else {
            eprintln!("Inserting...");
            item_to_insert.store(&db)?;
            eprintln!("Inserted, start again and it should be found");
        }
    }

    eprintln!("\nListing all items:");

    for (idx, item) in db.iter().into_iter().enumerate() {
        let (key, bytes) = item?;

        let mut item: Song = rmp_serde::from_slice(&bytes).expect("should deserialize");
        item.id = String::from_utf8_lossy(&key).to_string();

        eprintln!("[{idx}] {item}");
    }

    Ok(())
}
