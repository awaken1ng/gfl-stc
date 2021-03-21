use std::io;

#[derive(Debug)]
pub enum Error {
    IO(io::Error),

    // # deserialization
    LastBlockSizeMismatch,

    // # adding records, serialization
    /// Rows reached max capacity
    TableIsFull,

    /// Row has more than 255 fields
    TooManyFields,

    /// First field in the record must always be `i32`
    InvalidID,

    /// Inconsitent amount of colums in adding row
    InconsistentLength,

    /// String exceeded the 16-bit size limit
    StringTooBig,

    /// Bookmark out of bounds due to 32-bit limit
    OutOfBounds,

    // # access
    RowNotFound,

    ColumnNotFound,

    ConversionFailed,

    UnexpectedType,

    /// The length of resulting array does not match the requested length
    MismatchedLength,
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Self::IO(err)
    }
}

impl PartialEq for Error {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Error::IO(lhs), Error::IO(rhs)) => lhs.kind().eq(&rhs.kind()),
            (lhs, rhs) => lhs.eq(rhs),
        }
    }
}
