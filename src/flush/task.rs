use crate::Keyspace;

pub struct Task {
    pub(crate) keyspace: Keyspace,
}

impl std::fmt::Debug for Task {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FlushTask({})", self.keyspace.name)
    }
}
