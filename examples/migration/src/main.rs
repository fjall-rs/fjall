use fjall::Database;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct PlanetV1 {
    /// ID
    id: String,

    /// Name
    name: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct PlanetV2 {
    /// ID
    id: String,

    /// Name
    name: String,

    /// Mass
    mass: Option<u128>,

    /// Radius
    radius: Option<u128>,
}

impl From<PlanetV1> for PlanetV2 {
    fn from(value: PlanetV1) -> Self {
        Self {
            id: value.id,
            name: value.name,
            mass: None,
            radius: None,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub enum Planet {
    V1(PlanetV1),
    V2(PlanetV2),
}

impl std::fmt::Display for Planet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

fn main() -> fjall::Result<()> {
    let path = std::path::Path::new(".fjall_data");

    let db = Database::builder(path).temporary(true).open()?;
    let planets_v1 = db.keyspace("v1_planets", Default::default())?;

    for (id, name) in [
        ("p:earth", "Earth"),
        ("p:jupiter", "Jupiter"),
        ("p:neptune", "Mars"),
        ("p:neptune", "Mercury"),
        ("p:neptune", "Neptune"),
        ("p:saturn", "Saturn"),
        ("p:uranus", "Uranus"),
        ("p:venus", "Venus"),
    ] {
        planets_v1.insert(
            id,
            rmp_serde::to_vec(&Planet::V1(PlanetV1 {
                id: id.into(),
                name: name.into(),
            }))
            .unwrap(),
        )?;
    }

    eprintln!("--- v1 ---");
    for kv in planets_v1.iter() {
        let (_, v) = kv?;
        let planet = rmp_serde::from_slice::<Planet>(&v).unwrap();
        eprintln!("{planet:?}");
    }

    // Do migration from V1 -> V2
    let planets_v2 = db.keyspace("v2_planets", Default::default())?;

    let stream = planets_v1.iter().map(|kv| {
        let (k, v) = kv.unwrap();
        let planet = rmp_serde::from_slice::<Planet>(&v).unwrap();

        if let Planet::V1(planet) = planet {
            let v2: PlanetV2 = planet.into();
            let v2 = Planet::V2(v2);
            (k, rmp_serde::to_vec(&v2).unwrap())
        } else {
            unreachable!("v1 does not contain other versions");
        }
    });
    planets_v2.ingest(stream)?;

    eprintln!("--- v2 ---");
    for kv in planets_v2.iter() {
        let (_, v) = kv?;
        let planet = rmp_serde::from_slice::<Planet>(&v).unwrap();
        eprintln!("{planet:?}");
    }

    assert_eq!(planets_v1.len()?, planets_v2.len()?);

    // Delete old data
    db.delete_keyspace(planets_v1)?;

    Ok(())
}
