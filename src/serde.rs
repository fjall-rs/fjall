use std::io::{Read, Write};

// Custom error enum
#[derive(Debug)]
pub enum SerializeError {
    Io(std::io::Error),
}

// Custom error enum
#[derive(Debug)]
pub enum DeserializeError {
    Io(std::io::Error),
    CrcCheck(u32),
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

pub trait Serializable {
    // Serialize to bytes
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), SerializeError>;
}

pub trait Deserializable {
    // Deserialize from bytes
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, DeserializeError>
    where
        Self: Sized;
}
