use chrono::{Datelike, Timelike};
use rand::Rng;
use std::sync::Arc;

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
#[doc(hidden)]
#[must_use]
pub fn generate_segment_id() -> Arc<str> {
    let now = chrono::Utc::now();

    let year = now.year().unsigned_abs();
    let month = now.month() as u8;
    let day = (now.day() - 1) as u8;

    let hour = now.hour() as u8;
    let min = now.minute() as u8;

    let sec = now.second() as u8;
    let nano = now.timestamp_subsec_nanos();

    let mut rng = rand::thread_rng();
    let random = rng.gen::<u16>();

    format!(
        "{:0>4}_{}{}{:0>2}{:0>2}_{:0>2}{:0>8}_{:0>4}",
        to_base36(year),
        //
        to_base36(u32::from(month)),
        to_base36(u32::from(day)),
        to_base36(u32::from(hour)),
        to_base36(u32::from(min)),
        //
        to_base36(u32::from(sec)),
        to_base36(nano),
        //
        to_base36(u32::from(random)),
    )
    .into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_log::test;

    #[test]
    pub fn id_monotonic_order() {
        for _ in 0..1_000 {
            let ids = (0..100).map(|_| generate_segment_id()).collect::<Vec<_>>();

            let mut sorted = ids.clone();
            sorted.sort();

            assert_eq!(ids, sorted, "ID is not monotonic");
        }
    }
}
