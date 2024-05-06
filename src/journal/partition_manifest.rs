use lsm_tree::SeqNo;
use std::num::ParseIntError;

#[derive(Debug, Eq, PartialEq)]
pub struct Entry<'a> {
    pub(crate) partition_name: &'a str,
    pub(crate) seqno: SeqNo,
}

#[derive(Debug)]
pub enum ParseError {
    Io(std::io::Error),
    InvalidSeqno,
    Corrupted,
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PartitionManifestError: {self:?}")
    }
}

impl std::error::Error for ParseError {}

impl From<std::io::Error> for ParseError {
    fn from(inner: std::io::Error) -> Self {
        Self::Io(inner)
    }
}

impl From<ParseIntError> for ParseError {
    fn from(_: ParseIntError) -> Self {
        Self::InvalidSeqno
    }
}

pub fn parse(str: &str) -> Result<Vec<Entry>, ParseError> {
    str.split('\n')
        .filter(|x| !x.is_empty())
        .map(|x| {
            let mut splits = x.split(':');

            let Some(name) = splits.next() else {
                return Err(ParseError::Corrupted);
            };
            let Some(lsn) = splits.next() else {
                return Err(ParseError::Corrupted);
            };
            let lsn = lsn.parse::<lsm_tree::SeqNo>()?;

            Ok(Entry {
                partition_name: name,
                seqno: lsn,
            })
        })
        .collect::<Result<Vec<_>, _>>()
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_log::test;

    #[test]
    pub fn journal_parse_partition_manifest_success() -> Result<(), ParseError> {
        let str = r"
a:24
b:52
c:124";

        let entries = parse(str).expect("should parse");
        assert_eq!(
            [
                Entry {
                    partition_name: "a",
                    seqno: 24
                },
                Entry {
                    partition_name: "b",
                    seqno: 52
                },
                Entry {
                    partition_name: "c",
                    seqno: 124
                }
            ],
            *entries
        );

        Ok(())
    }

    #[test]
    pub fn journal_parse_partition_manifest_error() -> Result<(), ParseError> {
        let str = r"
a:24
b:asd
c:124";

        let entries = parse(str);
        assert!(entries.is_err());

        Ok(())
    }

    #[test]
    pub fn journal_parse_partition_manifest_error_2() -> Result<(), ParseError> {
        let str = r"
a:24
b:
c:124";

        let entries = parse(str);
        assert!(entries.is_err());

        Ok(())
    }

    #[test]
    pub fn journal_parse_partition_manifest_error_3() -> Result<(), ParseError> {
        let str = r"
a:24
   ads
c:124";

        let entries = parse(str);
        assert!(entries.is_err());

        Ok(())
    }

    #[test]
    pub fn journal_parse_partition_manifest_error_4() -> Result<(), ParseError> {
        let str = r"
a:24
b
c:124";

        let entries = parse(str);
        assert!(entries.is_err());

        Ok(())
    }
}
