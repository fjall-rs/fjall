use byteorder::{BigEndian, WriteBytesExt};
use serde::{Deserialize, Serialize};

#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Version {
    V0,
}

impl std::fmt::Display for Version {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "0")
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
    pub fn len() -> u8 {
        5
    }

    pub fn write_file_header<W: std::io::Write>(self, writer: &mut W) -> std::io::Result<usize> {
        writer.write_all(&[b'L', b'S', b'M'])?;
        writer.write_u16::<BigEndian>(u16::from(self))?;
        Ok(5)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_log::test;

    #[test]
    #[allow(clippy::expect_used)]
    pub fn test_version_len() {
        let mut buf = vec![];

        let size = Version::V0.write_file_header(&mut buf).expect("can't fail");

        assert_eq!(Version::len() as usize, size);
    }
}
