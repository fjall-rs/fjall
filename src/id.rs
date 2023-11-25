use chrono::{Datelike, Timelike};
use rand::Rng;

fn to_base36(mut x: u32) -> String {
    let mut result = vec![];

    loop {
        let m = x % 36;
        x /= 36;

        result.push(std::char::from_digit(m, 36).unwrap());
        if x == 0 {
            break;
        }
    }

    result.into_iter().rev().collect()
}

pub fn generate_table_id() -> String {
    // Like https://cassandra.apache.org/_/blog/Apache-Cassandra-4.1-New-SSTable-Identifiers.html
    let now = chrono::Utc::now();

    let month = now.month();
    let day = now.day();

    let hour = now.hour();
    let min = now.minute();

    let nano = now.timestamp_subsec_nanos();

    let mut rng = rand::thread_rng();
    let random = rng.gen::<u32>();

    format!(
        "{}{}_{}{}_{}_{}",
        to_base36(month),
        to_base36(day),
        to_base36(hour),
        to_base36(min),
        to_base36(nano),
        to_base36(random),
    )
}
