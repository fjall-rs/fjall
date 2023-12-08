//! Represents a single entry in the tree, which may or may not exist

use crate::{
    value::{UserData, UserKey},
    Tree,
};

/// Represents a missing entry in the tree
#[allow(clippy::module_name_repetitions)]
pub struct VacantEntry {
    pub(crate) tree: Tree,
    pub(crate) key: UserKey,
}

#[allow(clippy::module_name_repetitions)]
/// Represents an existing entry in the tree
pub struct OccupiedEntry {
    pub(crate) tree: Tree,
    pub(crate) key: UserKey,
    pub(crate) value: UserData,
}

impl OccupiedEntry {
    /// Gets the entry's value.
    #[must_use]
    pub fn get(&self) -> UserData {
        self.value.clone()
    }

    /// Updates the entry.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn update<V: AsRef<[u8]>>(&mut self, value: V) -> crate::Result<()> {
        self.value = value.as_ref().into();
        self.tree.insert(self.key.clone(), self.value.clone())?;
        Ok(())
    }
}

impl VacantEntry {
    /// Inserts the entry, making sure it exists and returns the value.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn insert<V: AsRef<[u8]>>(&self, value: V) -> crate::Result<UserData> {
        let value: UserData = value.as_ref().into();
        self.tree.insert(self.key.clone(), value.clone())?;
        Ok(value)
    }
}

/// Represents a single entry in the tree, which may or may not exist.
pub enum Entry {
    /// Represents a missing entry in the tree
    Vacant(VacantEntry),

    /// Represents an existing entry in the tree
    Occupied(OccupiedEntry),
}

use Entry::{Occupied, Vacant};

impl Entry {
    /// Returns a reference to this entry's key
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// use lsm_tree::{Config, Tree};
    ///
    /// let tree = Tree::open(Config::new(folder))?;
    ///
    /// let entry = tree.entry("a")?;
    /// assert_eq!("a".as_bytes(), &*entry.key());
    /// #
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    #[must_use]
    pub fn key(&self) -> UserKey {
        match self {
            Vacant(entry) => entry.key.clone(),
            Occupied(entry) => entry.key.clone(),
        }
    }

    /// Updates the value if it exists before any potential inserts.
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// use lsm_tree::{Config, Tree};
    ///
    /// let tree = Tree::open(Config::new(folder))?;
    ///
    /// let value = tree.entry("a")?.or_insert("abc")?;
    /// assert_eq!("abc".as_bytes(), &*value);
    ///
    /// let value = tree.entry("a")?.and_update(|_| "def")?.or_insert("abc")?;
    /// assert_eq!("def".as_bytes(), &*value);
    /// #
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn and_update<V: AsRef<[u8]>, F: FnOnce(&UserData) -> V>(
        mut self,
        f: F,
    ) -> crate::Result<Self> {
        if let Occupied(entry) = &mut self {
            entry.update(f(&entry.value).as_ref())?;
        }

        Ok(self)
    }

    /// Ensures a value is in the entry by inserting the default if empty, and returns that value.
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// use lsm_tree::{Config, Tree};
    ///
    /// let tree = Tree::open(Config::new(folder))?;
    ///
    /// let value = tree.entry("a")?.or_insert("abc")?;
    /// assert_eq!("abc".as_bytes(), &*value);
    /// #
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn or_insert<V: AsRef<[u8]>>(&self, value: V) -> crate::Result<UserData> {
        match self {
            Vacant(entry) => entry.insert(value),
            Occupied(entry) => Ok(entry.get()),
        }
    }

    /// Ensures a value is in the entry by inserting the result of the default function if empty, and returns that value.
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// use lsm_tree::{Config, Tree};
    ///
    /// let tree = Tree::open(Config::new(folder))?;
    ///
    /// let value = tree.entry("a")?.or_insert_with(|| "abc")?;
    /// assert_eq!("abc".as_bytes(), &*value);
    /// #
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn or_insert_with<V: AsRef<[u8]>, F: FnOnce() -> V>(
        &self,
        f: F,
    ) -> crate::Result<UserData> {
        match self {
            Vacant(entry) => entry.insert(f()),
            Occupied(entry) => Ok(entry.get()),
        }
    }

    /// Ensures a value is in the entry by inserting the result of the default function if empty, and returns that value.
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// use lsm_tree::{Config, Tree};
    ///
    /// let tree = Tree::open(Config::new(folder))?;
    ///
    /// let value = tree.entry("a")?.or_insert_with_key(|k| k.clone())?;
    /// assert_eq!("a".as_bytes(), &*value);
    /// #
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn or_insert_with_key<V: AsRef<[u8]>, F: FnOnce(&UserData) -> V>(
        &self,
        f: F,
    ) -> crate::Result<UserData> {
        match self {
            Vacant(entry) => entry.insert(f(&entry.key)),
            Occupied(entry) => Ok(entry.get()),
        }
    }
}
