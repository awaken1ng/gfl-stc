use std::{io, num::ParseIntError};

#[derive(Debug)]
pub enum Error {
    IO(io::Error),

    // # DEFINITIONS
    InvalidTableID(ParseIntError),

    NoTableName,

    NoTableColumnNames,

    NoTableColumnTypes,

    /// Column names and types lengths do not match
    InconsistentNamesAndTypesLength,

    // # DESERIALIZATION
    LastBlockSizeMismatch,

    // # ADDING ROWS, SERIALIZATION
    /// Rows reached max capacity
    TooManyRows,

    /// Row has more than 255 columns
    TooManyColumns,

    /// First column in the row must always be `i32`
    InvalidRowID,

    /// Inconsitent amount of colums in adding row
    InconsistentRowLength,

    /// String exceeded the 16-bit size limit
    StringTooBig,

    /// Bookmark out of bounds due to 32-bit limit
    BookmarkOutOfBounds,

    // # ACCESS
    RowNotFound,

    ColumnNotFound,

    ValueConversionFailed,

    InvalidColumnType,

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
