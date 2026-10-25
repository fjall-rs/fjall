/// The persist mode allows setting the durability guarantee of previous writes
#[derive(Copy, Clone, Default, Debug, Eq, PartialEq, Ord, PartialOrd)]
#[repr(u8)]
pub enum DurabilityLevel {
    /// Data is guaranteed to be visible to new readers,
    /// however it is not flushed to OS buffers.
    ///
    /// In the case of an application crash, the data may be lost.
    Visible = 0,

    /// Flushes data to OS buffers.
    /// This allows the OS to write out data in case of an application crash.
    ///
    /// Data is **not** guaranteed to be persisted in case
    /// of a power loss event or OS crash.
    #[default]
    Buffer = 1,

    /// Flushes data using `fdatasync`.
    ///
    /// Use if you know that `fdatasync` is sufficient for your file system and/or operating system.
    SyncData = 2,

    /// Flushes data + metadata using `fsync`.
    SyncAll = 3,
}
