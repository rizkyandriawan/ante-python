use std::fmt;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    QueueNotFound(String),
    QueueExists(String),
    PayloadTooLarge { size: usize, max: usize },
    BatchTooLarge { messages: usize, max: usize },
    BatchTooManyBytes { bytes: usize, max: usize },
    Invalid(String),
    Poisoned,
    Storage(Box<dyn std::error::Error + Send + Sync>),
}

impl Error {
    pub fn code(&self) -> &'static str {
        match self {
            Error::QueueNotFound(_) => "QUEUE_NOT_FOUND",
            Error::QueueExists(_) => "QUEUE_EXISTS",
            Error::PayloadTooLarge { .. } => "PAYLOAD_TOO_LARGE",
            Error::BatchTooLarge { .. } => "BATCH_TOO_LARGE",
            Error::BatchTooManyBytes { .. } => "BATCH_TOO_LARGE",
            Error::Invalid(_) => "INVALID",
            Error::Poisoned => "INTERNAL",
            Error::Storage(_) => "STORAGE",
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::QueueNotFound(q) => write!(f, "queue not found: {q}"),
            Error::QueueExists(q) => write!(f, "queue already exists: {q}"),
            Error::PayloadTooLarge { size, max } => {
                write!(f, "payload is {size} bytes, limit is {max}")
            }
            Error::BatchTooLarge { messages, max } => {
                write!(f, "batch has {messages} messages, limit is {max}")
            }
            Error::BatchTooManyBytes { bytes, max } => {
                write!(f, "batch is {bytes} bytes, limit is {max}")
            }
            Error::Invalid(msg) => write!(f, "invalid request: {msg}"),
            Error::Poisoned => write!(f, "storage lock poisoned"),
            Error::Storage(err) => write!(f, "storage error: {err}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Storage(err) => Some(err.as_ref()),
            _ => None,
        }
    }
}

macro_rules! from_storage {
    ($($ty:ty),* $(,)?) => {
        $(
            impl From<$ty> for Error {
                fn from(err: $ty) -> Self {
                    Error::Storage(Box::new(err))
                }
            }
        )*
    };
}

from_storage!(
    redb::DatabaseError,
    redb::TransactionError,
    redb::TableError,
    redb::StorageError,
    redb::CommitError,
);

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::Storage(Box::new(err))
    }
}
