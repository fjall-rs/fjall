use crate::Config;
use std::path::Path;

/// Transactional database builder
pub struct Builder(crate::DatabaseBuilder);

impl std::ops::Deref for Builder {
    type Target = crate::DatabaseBuilder;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for Builder {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Builder {
    pub(crate) fn new(path: &Path) -> Self {
        Self(crate::DatabaseBuilder::new(path))
    }

    #[doc(hidden)]
    #[must_use]
    pub fn into_config(self) -> Config {
        self.0.into_config()
    }

    /// Opens the database with transactional semantics, creating it if it does not exist.
    ///
    /// # Errors
    ///
    /// Errors if an I/O error occurred, or if the database can not be opened.
    pub fn open(self) -> crate::Result<crate::TxDatabase> {
        crate::TxDatabase::open(self.0.into_config())
    }
}
