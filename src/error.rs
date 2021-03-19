use std::io;

#[derive(Debug)]
pub enum ParsingError {
    IO(io::Error),

    LastBlockSizeMismatch,

    /// First field in the record must always be `i32`
    InvalidID,

    InconsistentLength,

    /// String exceeded the 16-bit size limit
    StringTooBig,

    /// Rows reached max capacity
    TableIsFull,

    /// Row has more than 255 fields
    TooManyFields,

    /// Bookmark out of bounds due to 32-bit limit
    OutOfBounds,
}

#[derive(Debug, PartialEq)]
pub enum AccessError {
    RowNotFound,

    ColumnNotFound,

    ConversionFailed,

    UnexpectedType,

    /// The length of resulting array does not match the requested length
    MismatchedLength,
}

impl From<io::Error> for ParsingError {
    fn from(err: io::Error) -> Self {
        Self::IO(err)
    }
}
