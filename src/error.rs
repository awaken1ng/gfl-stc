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

    // ! don't forget to add new variants to PartialEq
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Self::IO(err)
    }
}

impl PartialEq for Error {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Error::IO(lhs), Error::IO(rhs)) => lhs.kind() == rhs.kind(),
            (Error::InvalidTableID(lhs), Error::InvalidTableID(rhs)) => lhs == rhs,
            (Error::NoTableName, Error::NoTableName) => true,
            (Error::NoTableColumnNames, Error::NoTableColumnNames) => true,
            (Error::NoTableColumnTypes, Error::NoTableColumnTypes) => true,
            (Error::InconsistentNamesAndTypesLength, Error::InconsistentNamesAndTypesLength) => true,
            (Error::LastBlockSizeMismatch, Error::LastBlockSizeMismatch) => true,
            (Error::TooManyRows, Error::TooManyRows) => true,
            (Error::TooManyColumns, Error::TooManyColumns) => true,
            (Error::InvalidRowID, Error::InvalidRowID) => true,
            (Error::InconsistentRowLength, Error::InconsistentRowLength) => true,
            (Error::StringTooBig, Error::StringTooBig) => true,
            (Error::BookmarkOutOfBounds, Error::BookmarkOutOfBounds) => true,
            (Error::RowNotFound, Error::RowNotFound) => true,
            (Error::ColumnNotFound, Error::ColumnNotFound) => true,
            (Error::ValueConversionFailed, Error::ValueConversionFailed) => true,
            (Error::InvalidColumnType, Error::InvalidColumnType) => true,
            (Error::MismatchedLength, Error::MismatchedLength) => true,
            _ => false,
        }
    }
}
