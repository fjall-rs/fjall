use crate::prefix_to_range;
use lsm_tree::UserKey;
use std::ops::RangeBounds;

/// Helper function to create a prefixed range.
///
/// Made for phil.
///
/// # Panics
///
/// Panics if the prefix is empty.
#[doc(hidden)]
pub fn prefixed_range<P: AsRef<[u8]>, K: AsRef<[u8]>, R: RangeBounds<K>>(
    prefix: P,
    range: R,
) -> impl RangeBounds<UserKey> {
    use std::ops::Bound::{Excluded, Included, Unbounded};

    let prefix = prefix.as_ref();

    assert!(!prefix.is_empty(), "prefix may not be empty");

    match (range.start_bound(), range.end_bound()) {
        (Unbounded, Unbounded) => prefix_to_range(prefix),
        // TODO: use Bounds::map 1.77
        (lower, Unbounded) => {
            let prefix = UserKey::new(prefix);

            let lower = match lower {
                Included(k) => Included(prefix.concat(k.as_ref())),
                Excluded(k) => Excluded(prefix.concat(k.as_ref())),
                Unbounded => unreachable!(),
            };

            // TODO: does a heap allocation too much because we just throw away the lower bound
            let (_, upper) = prefix_to_range(&prefix);

            (lower, upper)
        }
        // TODO: use Bounds::map 1.77
        (Unbounded, upper) => {
            let prefix = UserKey::new(prefix);

            let upper = match upper {
                Included(k) => Included(prefix.concat(k.as_ref())),
                Excluded(k) => Excluded(prefix.concat(k.as_ref())),
                Unbounded => unreachable!(),
            };

            (Included(prefix), upper)
        }
        (lower, upper) => {
            // TODO: heap allocation kind of unnecessary if we had a better API to concat two byte slices into a Slice
            let prefix = UserKey::new(prefix);

            let lower = match lower {
                Included(k) => Included(prefix.concat(k.as_ref())),
                Excluded(k) => Excluded(prefix.concat(k.as_ref())),
                Unbounded => unreachable!(),
            };

            let upper = match upper {
                Included(k) => Included(prefix.concat(k.as_ref())),
                Excluded(k) => Excluded(prefix.concat(k.as_ref())),
                Unbounded => unreachable!(),
            };

            (lower, upper)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::prefixed_range;
    use crate::UserKey;
    use std::ops::Bound::{Excluded, Included};
    use std::ops::RangeBounds;
    use test_log::test;

    #[test]
    fn prefixed_range_1() {
        let prefix = "abc";
        let min = 5u8.to_be_bytes();
        let max = 9u8.to_be_bytes();

        let range = prefixed_range(prefix, min..=max);

        assert_eq!(
            range.start_bound(),
            Included(&UserKey::new(&[b'a', b'b', b'c', 5]))
        );
        assert_eq!(
            range.end_bound(),
            Included(&UserKey::new(&[b'a', b'b', b'c', 9]))
        );
    }

    #[test]
    fn prefixed_range_2() {
        let prefix = "abc";
        let min = 5u8.to_be_bytes();
        let max = 9u8.to_be_bytes();

        let range = prefixed_range(prefix, min..max);

        assert_eq!(
            range.start_bound(),
            Included(&UserKey::new(&[b'a', b'b', b'c', 5]))
        );
        assert_eq!(
            range.end_bound(),
            Excluded(&UserKey::new(&[b'a', b'b', b'c', 9]))
        );
    }

    #[test]
    fn prefixed_range_3() {
        let prefix = "abc";
        let min = 5u8.to_be_bytes();

        let range = prefixed_range(prefix, min..);

        assert_eq!(
            range.start_bound(),
            Included(&UserKey::new(&[b'a', b'b', b'c', 5]))
        );
        assert_eq!(range.end_bound(), Excluded(&UserKey::new(b"abd")));
    }

    #[test]
    fn prefixed_range_4() {
        let prefix = "abc";
        let max = 9u8.to_be_bytes();

        let range = prefixed_range(prefix, ..max);

        assert_eq!(range.start_bound(), Included(&UserKey::new(b"abc")));
        assert_eq!(
            range.end_bound(),
            Excluded(&UserKey::new(&[b'a', b'b', b'c', 9]))
        );
    }

    #[test]
    fn prefixed_range_5() {
        let prefix = "abc";
        let max = u8::MAX.to_be_bytes();

        let range = prefixed_range(prefix, ..=max);

        assert_eq!(range.start_bound(), Included(&UserKey::new(b"abc")));
        assert_eq!(
            range.end_bound(),
            Included(&UserKey::new(&[b'a', b'b', b'c', u8::MAX]))
        );
    }

    #[test]
    fn prefixed_range_6() {
        let prefix = "abc";
        let max = u8::MAX.to_be_bytes();

        let range = prefixed_range(prefix, ..max);

        assert_eq!(range.start_bound(), Included(&UserKey::new(b"abc")));
        assert_eq!(
            range.end_bound(),
            Excluded(&UserKey::new(&[b'a', b'b', b'c', u8::MAX]))
        );
    }
}
