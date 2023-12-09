use lsm_tree::Tree as LsmTree;
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

impl Song {
    pub fn store(&self, tree: &LsmTree) -> lsm_tree::Result<()> {
        let serialized = rmp_serde::to_vec(self).expect("should serialize");
        tree.insert(&self.id, serialized)?;
        Ok(())
    }

    pub fn load(tree: &LsmTree, key: &str) -> lsm_tree::Result<Option<Song>> {
        let Some(item) = tree.get(key)? else {
            return Ok(None);
        };
        let mut item: Song = rmp_serde::from_slice(&item).expect("should deserialize");
        item.id = key.to_owned();
        Ok(Some(item))
    }
}

fn main() -> lsm_tree::Result<()> {
    let item_to_insert = Song {
        id: "clairo:amoeba".to_owned(),
        release_year: 2021,
        artist: "Clairo".to_owned(),
        title: "Amoeba".to_owned(),
    };

    let tree = lsm_tree::Config::default().open()?;

    if let Some(item) = Song::load(&tree, "clairo:amoeba")? {
        eprintln!("Found: {item:#?}");

        assert_eq!(item, item_to_insert);
    } else {
        eprintln!("Inserting...");
        item_to_insert.store(&tree)?;
        tree.flush()?; // Tree flushes on drop anyway, but just to be nice
        eprintln!("Inserted, start again and it should be found");
    }

    Ok(())
}
