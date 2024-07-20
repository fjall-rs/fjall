use path_absolutize::Absolutize;
use std::path::{Path, PathBuf};

#[allow(clippy::module_name_repetitions)]
pub fn absolute_path<P: AsRef<Path>>(path: P) -> PathBuf {
    // TODO: replace with https://doc.rust-lang.org/std/path/fn.absolute.html once stable
    path.as_ref()
        .absolutize()
        .expect("should be absolute path")
        .into()
}
