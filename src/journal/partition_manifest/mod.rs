// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

mod error;

pub use error::Error;

#[derive(Debug, Eq, PartialEq)]
pub struct Entry<'a> {
    pub(crate) partition_name: &'a str,
    pub(crate) seqno: lsm_tree::SeqNo,
}

#[derive(Debug, Eq, PartialEq)]
pub struct PartitionManifest<'a>(Vec<Entry<'a>>);

impl<'a> std::ops::Deref for PartitionManifest<'a> {
    type Target = Vec<Entry<'a>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a> IntoIterator for PartitionManifest<'a> {
    type IntoIter = std::vec::IntoIter<Entry<'a>>;
    type Item = Entry<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a> PartitionManifest<'a> {
    pub fn from_str(s: &'a str) -> Result<Self, Error> {
        let entries = s
            .split('\n')
            .filter(|x| !x.is_empty())
            .map(|x| {
                let mut splits = x.split(':');

                let Some(name) = splits.next() else {
                    return Err(Error::Corrupted);
                };
                let Some(lsn) = splits.next() else {
                    return Err(Error::Corrupted);
                };
                let lsn = lsn.parse::<lsm_tree::SeqNo>()?;

                Ok(Entry {
                    partition_name: name,
                    seqno: lsn,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self(entries))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_log::test;

    #[test]
    pub fn journal_parse_partition_manifest_success() -> Result<(), Error> {
        let str = r"
a:24
b:52
c:124";

        let entries = PartitionManifest::from_str(str)?;
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
            **entries
        );

        Ok(())
    }

    #[test]
    pub fn journal_parse_partition_manifest_error() {
        let str = r"
a:24
b:asd
c:124";

        let entries = PartitionManifest::from_str(str);
        assert!(entries.is_err());
    }

    #[test]
    pub fn journal_parse_partition_manifest_error_2() {
        let str = r"
a:24
b:
c:124";

        let entries = PartitionManifest::from_str(str);
        assert!(entries.is_err());
    }

    #[test]
    pub fn journal_parse_partition_manifest_error_3() {
        let str = r"
a:24
   ads
c:124";

        let entries = PartitionManifest::from_str(str);
        assert!(entries.is_err());
    }

    #[test]
    pub fn journal_parse_partition_manifest_error_4() {
        let str = r"
a:24
b
c:124";

        let entries = PartitionManifest::from_str(str);
        assert!(entries.is_err());
    }
}
