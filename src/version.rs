use byteorder::{BigEndian, WriteBytesExt};
use serde::{Deserialize, Serialize};

#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Version {
    V0,
}

impl std::fmt::Display for Version {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "1")
    }
}

impl From<Version> for u16 {
    fn from(value: Version) -> Self {
        match value {
            Version::V0 => 0,
        }
    }
}

impl Version {
    pub fn write_file_header<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<usize> {
        writer.write_all(&[b'L', b'S', b'M'])?;
        writer.write_u16::<BigEndian>(u16::from(*self))?;
        Ok(5)
    }
}
