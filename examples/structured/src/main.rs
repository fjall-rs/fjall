use fjall::{Config, Keyspace, PartitionHandle};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Song {
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

impl From<&Song> for Vec<u8> {
    fn from(val: &Song) -> Self {
        rmp_serde::to_vec(&val).expect("should serialize")
    }
}

impl From<(Arc<[u8]>, Arc<[u8]>)> for Song {
    fn from((key, value): (Arc<[u8]>, Arc<[u8]>)) -> Self {
        let key = std::str::from_utf8(&key).unwrap();
        let mut item: Song = rmp_serde::from_slice(&value).expect("should deserialize");
        key.clone_into(&mut item.id);
        item
    }
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

pub struct SongDatabase {
    #[allow(unused)]
    keyspace: Keyspace,

    db: PartitionHandle,
}

impl SongDatabase {
    pub fn get(&self, key: &str) -> fjall::Result<Option<Song>> {
        let Some(item) = self.db.get(key)? else {
            return Ok(None);
        };

        let mut song: Song = rmp_serde::from_slice(&item).expect("should deserialize");
        key.clone_into(&mut song.id);

        Ok(Some(song))
    }

    pub fn insert(&self, song: &Song) -> fjall::Result<()> {
        let serialized: Vec<u8> = song.into();
        self.db.insert(&song.id, serialized)
    }

    pub fn iter(&self) -> impl Iterator<Item = fjall::Result<Song>> + '_ {
        self.db.iter().map(|item| item.map(Song::from))
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
    let db = keyspace.open_partition("songs", Default::default())?;

    let song_db = SongDatabase { keyspace, db };

    for item_to_insert in items {
        if let Some(item) = song_db.get(&item_to_insert.id)? {
            println!("Found: {item}");
            assert_eq!(item, item_to_insert);
        } else {
            println!("Inserting...");
            song_db.insert(&item_to_insert)?;
            println!("Inserted, start again and it should be found");
        }
    }

    println!("\nListing all items:");

    for (idx, song) in song_db.iter().enumerate() {
        let song = song?;
        println!("[{idx}] {song}");
    }

    Ok(())
}
