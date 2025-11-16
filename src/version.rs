// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use byteorder::WriteBytesExt;

/// Disk format version
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum FormatVersion {
    /// Version for 1.x.x releases
    V1,

    /// Version for 2.x.x releases
    V2,

    /// Version for 3.x.x releases
    V3,
}

impl std::fmt::Display for FormatVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", u8::from(*self))
    }
}

impl From<FormatVersion> for u8 {
    fn from(value: FormatVersion) -> Self {
        match value {
            FormatVersion::V1 => 1,
            FormatVersion::V2 => 2,
            FormatVersion::V3 => 3,
        }
    }
}

impl TryFrom<u8> for FormatVersion {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::V1),
            2 => Ok(Self::V2),
            3 => Ok(Self::V3),
            _ => Err(()),
        }
    }
}

const MAGIC_BYTES: [u8; 3] = *b"FJA";

impl FormatVersion {
    pub(crate) fn parse_file_header(bytes: &[u8]) -> Option<Self> {
        let first_three = bytes.get(0..3)?;

        if first_three == MAGIC_BYTES {
            let version = *bytes.get(3)?;
            let version = Self::try_from(version).ok()?;
            Some(version)
        } else {
            None
        }
    }

    pub(crate) fn write_file_header<W: std::io::Write>(
        self,
        writer: &mut W,
    ) -> std::io::Result<()> {
        writer.write_all(&MAGIC_BYTES)?;
        writer.write_u8(u8::from(self))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_log::test;

    #[test]
    #[allow(clippy::expect_used)]
    pub fn version_serialize() -> crate::Result<()> {
        let mut bytes = vec![];
        FormatVersion::V1.write_file_header(&mut bytes)?;
        assert_eq!(bytes, &[b'F', b'J', b'L', 1]);
        Ok(())
    }

    #[test]
    #[allow(clippy::expect_used)]
    pub fn version_serialize_2() -> crate::Result<()> {
        let mut bytes = vec![];
        FormatVersion::V2.write_file_header(&mut bytes)?;
        assert_eq!(bytes, &[b'F', b'J', b'L', 2]);
        Ok(())
    }

    #[test]
    #[allow(clippy::expect_used)]
    pub fn version_deserialize_success() {
        let version = FormatVersion::parse_file_header(&[b'F', b'J', b'L', 1]);
        assert_eq!(version, Some(FormatVersion::V1));
    }

    #[test]
    #[allow(clippy::expect_used)]
    pub fn version_deserialize_success_2() {
        let version = FormatVersion::parse_file_header(&[b'F', b'J', b'L', 2]);
        assert_eq!(version, Some(FormatVersion::V2));
    }

    #[test]
    #[allow(clippy::expect_used)]
    pub fn version_deserialize_fail() {
        let version = FormatVersion::parse_file_header(&[b'F', b'J', b'X', 1]);
        assert!(version.is_none());
    }

    #[test]
    #[allow(clippy::expect_used)]
    pub fn version_serde_round_trip() {
        let mut buf = vec![];
        FormatVersion::V1
            .write_file_header(&mut buf)
            .expect("can't fail");

        let version = FormatVersion::parse_file_header(&buf);
        assert_eq!(version, Some(FormatVersion::V1));
    }

    #[test]
    #[allow(clippy::expect_used)]
    pub fn version_len() {
        let mut buf = vec![];
        FormatVersion::V1
            .write_file_header(&mut buf)
            .expect("can't fail");
        assert_eq!(4, buf.len());
    }
}
