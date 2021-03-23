use std::{collections::HashMap, convert::TryFrom, hash::Hash, io, str::FromStr};

use crate::{definitions::TableDefinition, table::Table, Error, Value};

pub struct NamedTable {
    pub name: String,
    // mapping from id column to row index
    id_to_index: HashMap<i32, usize>,
    // mapping from column name to column index
    column_to_index: HashMap<String, usize>,
    pub table: Table,
}

impl NamedTable {
    /// SAFETY panics if first column in row is not i32
    pub fn from_definition(table: Table, def: &TableDefinition) -> Self {
        let column_to_index: HashMap<String, usize> = def
            .columns
            .clone()
            .into_iter()
            .enumerate()
            .map(|(i, n)| (n, i))
            .collect();

        let id_to_index: HashMap<i32, usize> = table
            .rows
            .iter()
            .enumerate()
            .map(|(i, row)| {
                (
                    row.get(0)
                        .map(Value::as_i32)
                        .flatten()
                        .expect("first column missing or not i32"),
                    i,
                )
            })
            .collect();

        Self {
            name: def.name.clone(),
            column_to_index,
            id_to_index,
            table,
        }
    }

    #[cfg(feature = "csv")]
    /// Read the table from .csv, reader must start with column types
    pub fn from_csv<R>(id: u16, reader: R, def: &TableDefinition) -> Result<Self, Error>
    where
        R: io::Read,
    {
        let table = Table::from_csv(id, reader)?;
        Ok(Self::from_definition(table, def))
    }

    #[cfg(feature = "csv")]
    pub fn to_csv<W>(&self, writer: W, with_names: bool, with_types: bool) -> Result<W, Error>
    where
        W: io::Write,
    {
        if self.table.rows.is_empty() {
            return Ok(writer);
        }

        let mut writer = csv::Writer::from_writer(writer);

        let first = self.table.rows.first().unwrap(); // SAFETY checked earlier

        if with_names {
            let column_names = first.iter().enumerate().map(|(i, _)| format!("col-{}", i));
            writer.write_record(column_names)?;
        }

        writer.flush()?;
        let writer = writer.into_inner().unwrap();

        self.table.to_csv(writer, false, with_types)
    }

    pub fn value<'a, T>(&'a self, row_id: i32, column_name: &str) -> Result<T, Error>
    where
        T: TryFrom<&'a Value>,
    {
        let row_index = self.id_to_index.get(&row_id).ok_or(Error::RowNotFound)?;
        let column_index = self
            .column_to_index
            .get(column_name)
            .ok_or(Error::ColumnNotFound)?;
        self.table.value(*row_index, *column_index)
    }

    pub fn array<'a, T>(
        &'a self,
        row_id: i32,
        column_name: &str,
        separator: &str,
        length: usize,
    ) -> Result<Vec<T>, Error>
    where
        T: FromStr,
    {
        let ret = self.vector(row_id, column_name, separator)?;

        if ret.len() != length {
            Err(Error::MismatchedLength)
        } else {
            Ok(ret)
        }
    }

    pub fn vector<'a, T>(
        &'a self,
        row_id: i32,
        column_name: &str,
        separator: &str,
    ) -> Result<Vec<T>, Error>
    where
        T: FromStr,
    {
        let row_index = self.id_to_index.get(&row_id).ok_or(Error::RowNotFound)?;
        let column_index = self
            .column_to_index
            .get(column_name)
            .ok_or(Error::ColumnNotFound)?;
        self.table.vector(*row_index, *column_index, separator)
    }

    pub fn map<K, V>(
        &self,
        row_id: i32,
        column_name: &str,
        pair_separator: &str,
        kv_separator: &str,
    ) -> Result<HashMap<K, V>, Error>
    where
        K: FromStr + Eq + Hash,
        V: FromStr,
    {
        let row_index = self.id_to_index.get(&row_id).ok_or(Error::RowNotFound)?;
        let column_index = self
            .column_to_index
            .get(column_name)
            .ok_or(Error::ColumnNotFound)?;
        self.table
            .map(*row_index, *column_index, pair_separator, kv_separator)
    }
}
