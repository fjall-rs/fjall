use std::{fs::File, path::Path, sync::Arc};

struct LockedFileGuardInner(File);

impl Drop for LockedFileGuardInner {
    fn drop(&mut self) {
        log::debug!("Unlocking database lock");

        self.0
            .unlock()
            .inspect_err(|e| {
                log::warn!("Failed to unlock database lock: {e:?}");
            })
            .ok();
    }
}

#[derive(Clone)]
#[allow(unused)]
pub struct LockedFileGuard(Arc<LockedFileGuardInner>);

impl LockedFileGuard {
    pub fn create_new(path: &Path) -> crate::Result<Self> {
        log::debug!("Acquiring database lock at {}", path.display());

        let file = File::create_new(path)?;

        file.try_lock().map_err(|e| match e {
            std::fs::TryLockError::Error(e) => {
                log::error!("Failed to acquire database lock - if this is expected, you can try opening again (maybe wait a little)");
                crate::Error::Io(e)
            }
            std::fs::TryLockError::WouldBlock => crate::Error::Locked,
        })?;

        Ok(Self(Arc::new(LockedFileGuardInner(file))))
    }

    pub fn try_acquire(path: &Path) -> crate::Result<Self> {
        log::debug!("Acquiring database lock at {}", path.display());

        let file = File::open(path)?;

        file.try_lock().map_err(|e| match e {
            std::fs::TryLockError::Error(e) => {
                log::error!("Failed to acquire database lock - if this is expected, you can try opening again (maybe wait a little)");
                crate::Error::Io(e)
            }
            std::fs::TryLockError::WouldBlock => crate::Error::Locked,
        })?;

        Ok(Self(Arc::new(LockedFileGuardInner(file))))
    }
}
