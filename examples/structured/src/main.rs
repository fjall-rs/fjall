use fjall::{Database, Guard, Keyspace, UserKey, UserValue};
use serde::{Deserialize, Serialize};

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

impl From<(UserKey, UserValue)> for Song {
    fn from((key, value): (UserKey, UserValue)) -> Self {
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
    db: Database,

    tree: Keyspace,
}

impl SongDatabase {
    pub fn get(&self, key: &str) -> fjall::Result<Option<Song>> {
        let Some(item) = self.tree.get(key)? else {
            return Ok(None);
        };

        let mut song: Song = rmp_serde::from_slice(&item).expect("should deserialize");
        key.clone_into(&mut song.id);

        Ok(Some(song))
    }

    pub fn insert(&self, song: &Song) -> fjall::Result<()> {
        let serialized: Vec<u8> = song.into();
        self.tree.insert(&song.id, serialized)
    }

    pub fn iter(&self) -> impl Iterator<Item = fjall::Result<Song>> + '_ {
        self.tree
            .iter()
            .map(|kv| kv.into_inner().map_err(Into::into)) // TODO: 3.0.0 should not expose lsm-tree error
            .map(|item| item.map(Song::from))
    }

    pub fn len(&self) -> fjall::Result<usize> {
        self.tree.len()
    }
}

fn main() -> fjall::Result<()> {
    let path = std::path::Path::new(".fjall_data");

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

    {
        let db = Database::builder(path).open()?;
        let tree = db.keyspace("songs", Default::default())?;

        let song_db = SongDatabase {
            db: db.clone(),
            tree,
        };

        for item_to_insert in &items {
            if let Some(item) = song_db.get(&item_to_insert.id)? {
                println!("Found: {item}");
                assert_eq!(&item, item_to_insert);
            } else {
                println!("Inserting...");
                song_db.insert(item_to_insert)?;
            }
        }
        db.persist(fjall::PersistMode::SyncAll)?;

        assert_eq!(items.len(), song_db.len()?);
    }

    // Reload from disk
    {
        println!("\nReloading...");

        let db = Database::builder(path).temporary(true).open()?;
        let tree = db.keyspace("songs", Default::default())?;

        let song_db = SongDatabase {
            db: db.clone(),
            tree,
        };

        println!("\nListing all items:");

        for (idx, song) in song_db.iter().enumerate() {
            let song = song?;
            println!("[{idx}] {song}");
        }

        assert_eq!(items.len(), song_db.len()?);
    }

    Ok(())
}
