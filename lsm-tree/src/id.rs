use std::sync::Arc;

use chrono::{Datelike, Timelike};
use rand::Rng;

const BASE_36_RADIX: u32 = 36;

fn to_base36(mut x: u32) -> String {
    let mut result = vec![];

    loop {
        let m = x % BASE_36_RADIX;
        x /= BASE_36_RADIX;

        result.push(std::char::from_digit(m, BASE_36_RADIX).expect("should be hex digit"));
        if x == 0 {
            break;
        }
    }

    result.into_iter().rev().collect()
}

/// Generates an ID for a segment
///
/// Like <https://cassandra.apache.org/_/blog/Apache-Cassandra-4.1-New-SSTable-Identifiers.html>
#[allow(clippy::module_name_repetitions)]
#[must_use]
pub fn generate_segment_id() -> Arc<str> {
    let now = chrono::Utc::now();

    let year = now.year().unsigned_abs();
    let month = now.month();
    let day = now.day();

    let hour = now.hour();
    let min = now.minute();

    let nano = now.timestamp_subsec_nanos();

    let mut rng = rand::thread_rng();
    let random = rng.gen::<u32>();

    format!(
        "{}_{}{}_{}{}_{}_{}",
        to_base36(year),
        to_base36(month),
        to_base36(day),
        to_base36(hour),
        to_base36(min),
        to_base36(nano),
        to_base36(random),
    )
    .into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_log::test;

    #[test]
    pub fn id_order() {
        let ids = [
            generate_segment_id(),
            generate_segment_id(),
            generate_segment_id(),
            generate_segment_id(),
            generate_segment_id(),
            generate_segment_id(),
            generate_segment_id(),
            generate_segment_id(),
        ];
        let mut sorted = ids.clone();
        sorted.sort();

        assert_eq!(ids, sorted);
    }
}
