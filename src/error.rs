use std::{io, num::ParseIntError};

#[derive(Debug)]
pub enum Error {
    IO(io::Error),

    #[cfg(feature = "csv")]
    CSV(csv::Error),

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

#[cfg(feature = "csv")]
impl From<csv::Error> for Error {
    fn from(err: csv::Error) -> Self {
        Self::CSV(err)
    }
}
