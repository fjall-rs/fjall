const VALID_CHARACTERS: &str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-";

pub fn is_valid_partition_name(s: &str) -> bool {
    s.chars().all(|c| VALID_CHARACTERS.contains(c))
}
