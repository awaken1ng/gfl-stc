use std::{collections::HashMap, convert::TryFrom, hash::Hash, str::FromStr};

use crate::{Error, Value, definitions::TableDefinition, table::Table};

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

    pub fn value<'a, T>(&'a self, row_id: i32, column_name: &str) -> Result<T, Error>
    where
        T: TryFrom<&'a Value>,
    {
        let row_index = self
            .id_to_index
            .get(&row_id)
            .ok_or(Error::RowNotFound)?;
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
        let row_index = self
            .id_to_index
            .get(&row_id)
            .ok_or(Error::RowNotFound)?;
        let column_index = self
            .column_to_index
            .get(column_name)
            .ok_or(Error::ColumnNotFound)?;
        self.table.array(*row_index, *column_index, separator)
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
        let row_index = self
            .id_to_index
            .get(&row_id)
            .ok_or(Error::RowNotFound)?;
        let column_index = self
            .column_to_index
            .get(column_name)
            .ok_or(Error::ColumnNotFound)?;
        self.table
            .map(*row_index, *column_index, pair_separator, kv_separator)
    }
}
