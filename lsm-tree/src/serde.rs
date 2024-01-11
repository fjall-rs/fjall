use std::io::{Read, Write};

/// Error during serialization
#[derive(Debug)]
pub enum SerializeError {
    /// I/O error
    Io(std::io::Error),
}

/// Error during deserialization
#[derive(Debug)]
pub enum DeserializeError {
    /// I/O error
    Io(std::io::Error),

    /// Invalid enum tag
    InvalidTag(u8),

    /// Invalid trailer
    InvalidTrailer,
}

impl From<std::io::Error> for SerializeError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<std::io::Error> for DeserializeError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

/// Trait to serialize stuff
pub trait Serializable {
    /// Serialize to bytes
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), SerializeError>;
}

/// Trait to deserialize stuff
pub trait Deserializable {
    /// Deserialize from bytes
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, DeserializeError>
    where
        Self: Sized;
}
