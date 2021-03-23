use std::{
    collections::HashMap,
    convert::{TryFrom, TryInto},
    hash::Hash,
    io::{self, Read, Seek, SeekFrom},
    ops::Deref,
    str::FromStr,
};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use crate::{Error, Value};

pub type Row = Vec<Value>;

#[derive(Debug, Clone)]
pub struct Table {
    pub id: u16,
    pub rows: Vec<Row>,
}

impl Table {
    pub fn new(id: u16) -> Self {
        Self {
            id,
            rows: Vec::new(),
        }
    }

    pub fn add_row(&mut self, row: Vec<Value>) -> Result<(), Error> {
        if self.rows.len() >= u16::MAX.into() {
            return Err(Error::TooManyRows);
        }

        if row.len() > u8::MAX.into() {
            return Err(Error::TooManyColumns);
        }

        // first value must be i32
        match row.first() {
            Some(Value::I32(_)) => {}
            _ => return Err(Error::InvalidRowID),
        }

        if let Some(first) = self.rows.first() {
            if first.len() != row.len() {
                return Err(Error::InconsistentRowLength);
            }
        }

        self.rows.push(row);

        Ok(())
    }

    pub fn deserialize<R>(reader: &mut R) -> Result<Self, Error>
    where
        R: Read + Seek,
    {
        let table_id = reader.read_u16::<LittleEndian>()?;
        let last_block_size: u64 = reader.read_u16::<LittleEndian>()?.into(); // size of the last 65kb block
        let rows = reader.read_u16::<LittleEndian>()?;

        let mut table = Self::new(table_id);

        if rows == 0 {
            return Ok(table);
        }

        let columns: usize = reader.read_u8()?.into();
        let mut column_types = Vec::with_capacity(columns);
        for _ in 0..columns {
            let t = reader.read_u8()?;
            column_types.push(t);
        }

        // read jump table
        let _first_row_id = reader.read_i32::<LittleEndian>()?;
        let first_row_offset: u64 = reader.read_u32::<LittleEndian>()?.into();

        // skip the rest of the table
        reader.seek(SeekFrom::Start(first_row_offset))?;

        for _ in 0..rows {
            let mut row = Vec::with_capacity(columns);

            for t in &column_types {
                row.push(Value::read(*t, reader)?);
            }

            table.rows.push(row);
        }

        let cur_pos = reader.seek(SeekFrom::Current(0))?;
        if last_block_size != (cur_pos - 4) % 65536 {
            return Err(Error::LastBlockSizeMismatch);
        }

        Ok(table)
    }

    pub fn serialize<W>(&self, writer: &mut W) -> Result<(), Error>
    where
        W: WriteBytesExt + Seek,
    {
        writer.write_u16::<LittleEndian>(self.id)?;

        writer.write_u16::<LittleEndian>(2)?; // lbs placeholder, current position

        let rows_n = self.rows.len().try_into().map_err(|_| Error::TooManyRows)?;

        writer.write_u16::<LittleEndian>(rows_n)?;

        if self.rows.is_empty() {
            return Ok(());
        }

        // SAFETY checked above
        let first = self.rows.first().unwrap();

        let columns_n: u8 = first.len().try_into().map_err(|_| Error::TooManyColumns)?;
        writer.write_u8(columns_n)?;

        // column types
        for v in first.iter() {
            writer.write_u8(v.type_as_u8())?;
        }

        // jump table placeholder
        let jump_table_size = 1 + (self.rows.len() / 100);
        for _ in 0..jump_table_size {
            writer.write_i32::<LittleEndian>(0)?; // id
            writer.write_u32::<LittleEndian>(0)?; // offset
        }

        let mut jump_table = Vec::with_capacity(jump_table_size);

        for (row_i, row) in self.rows.iter().enumerate() {
            for (column_i, column) in row.iter().enumerate() {
                if row_i % 100 == 0 && column_i == 0 {
                    let id = column.as_i32().ok_or(Error::InvalidRowID)?;
                    let pos: u32 = writer
                        .seek(SeekFrom::Current(0))?
                        .try_into()
                        .map_err(|_| Error::BookmarkOutOfBounds)?;

                    jump_table.push((id, pos));
                }

                column.serialize(writer)?;
            }
        }

        let lbs = (writer.seek(SeekFrom::Current(0))? - 4) % 65536;
        writer.seek(SeekFrom::Start(2))?;
        writer.write_u16::<LittleEndian>(lbs as u16)?;

        assert_eq!(jump_table.len(), jump_table_size);

        // seek to the start of the jump table
        // id (2), lbs (2), rows_n (2), columns_n (1), column_types (columns_n)
        writer.seek(SeekFrom::Start(7 + u64::from(columns_n)))?;
        for (id, offset) in jump_table {
            writer.write_i32::<LittleEndian>(id)?;
            writer.write_u32::<LittleEndian>(offset)?;
        }

        writer.flush()?;

        Ok(())
    }

    #[cfg(feature = "csv")]
    /// Read the table from .csv, reader must start with column types
    pub fn from_csv<R>(id: u16, reader: R) -> Result<Self, Error>
    where
        R: io::Read,
    {
        let mut reader = csv::ReaderBuilder::default()
            .has_headers(false)
            .from_reader(reader);

        let mut types = csv::StringRecord::new();
        reader.read_record(&mut types)?;
        let types: Vec<&str> = types.iter().collect();

        let mut table = Self {
            id,
            rows: Vec::new(),
        };
        for record in reader.records() {
            let row = record?;
            let row: Result<Vec<Value>, _> = row
                .iter()
                .enumerate()
                .map(|(i, col)| {
                    let col_type = types.get(i).ok_or(Error::InconsistentNamesAndTypesLength)?;
                    match col_type.deref() {
                        "i8" => col
                            .parse()
                            .map(Value::U8)
                            .map_err(|_| Error::ValueConversionFailed),
                        "u8" => col
                            .parse()
                            .map(Value::U8)
                            .map_err(|_| Error::ValueConversionFailed),
                        "i16" => col
                            .parse()
                            .map(Value::I16)
                            .map_err(|_| Error::ValueConversionFailed),
                        "u16" => col
                            .parse()
                            .map(Value::U16)
                            .map_err(|_| Error::ValueConversionFailed),
                        "i32" => col
                            .parse()
                            .map(Value::I32)
                            .map_err(|_| Error::ValueConversionFailed),
                        "u32" => col
                            .parse()
                            .map(Value::U32)
                            .map_err(|_| Error::ValueConversionFailed),
                        "i64" => col
                            .parse()
                            .map(Value::I64)
                            .map_err(|_| Error::ValueConversionFailed),
                        "u64" => col
                            .parse()
                            .map(Value::U64)
                            .map_err(|_| Error::ValueConversionFailed),
                        "f32" => col
                            .parse()
                            .map(Value::F32)
                            .map_err(|_| Error::ValueConversionFailed),
                        "f64" => col
                            .parse()
                            .map(Value::F64)
                            .map_err(|_| Error::ValueConversionFailed),
                        "string" => Ok(Value::String(col.to_owned())),
                        _ => Err(Error::InvalidColumnType),
                    }
                })
                .collect();
            let row = row?;
            table.add_row(row)?;
        }

        Ok(table)
    }

    #[cfg(feature = "csv")]
    pub fn to_csv<W>(&self, writer: W, with_names: bool, with_types: bool) -> Result<W, Error>
    where
        W: io::Write,
    {
        if self.rows.is_empty() {
            return Ok(writer);
        }

        let mut writer = csv::Writer::from_writer(writer);

        let first = self.rows.first().unwrap(); // SAFETY checked earlier

        if with_names {
            let column_names = first.iter().enumerate().map(|(i, _)| format!("col-{}", i));
            writer.write_record(column_names)?;
        }

        if with_types {
            let column_types = first.iter().map(Value::type_as_string);
            writer.write_record(column_types)?;
        }

        for row in self.rows.iter() {
            let stringified = row.iter().map(ToString::to_string);
            writer.write_record(stringified)?;
        }

        // SAFETY should not panic, unless second flush somehow fails
        writer.flush()?;
        let writer = writer.into_inner().unwrap();
        Ok(writer)
    }

    pub fn value<'a, T>(&'a self, row: usize, column: usize) -> Result<T, Error>
    where
        T: TryFrom<&'a Value>,
    {
        let row = self.rows.get(row).ok_or(Error::RowNotFound)?;
        let column = row.get(column).ok_or(Error::ColumnNotFound)?;

        T::try_from(column).map_err(|_| Error::ValueConversionFailed)
    }

    /// Convert `"v,v,v"` string into `Vec<T>`
    pub fn vector<T>(&self, row: usize, column: usize, separator: &str) -> Result<Vec<T>, Error>
    where
        T: FromStr,
    {
        let row = self.rows.get(row).ok_or(Error::RowNotFound)?;
        let column = row.get(column).ok_or(Error::ColumnNotFound)?;

        match column {
            Value::String(string) => string
                .split(separator)
                .map(T::from_str)
                .collect::<Result<Vec<T>, _>>()
                .map_err(|_| Error::ValueConversionFailed),
            _ => Err(Error::InvalidColumnType),
        }
    }

    pub fn map<K, V>(
        &self,
        row: usize,
        column: usize,
        pair_separator: &str,
        kv_separator: &str,
    ) -> Result<HashMap<K, V>, Error>
    where
        K: FromStr + Eq + Hash,
        V: FromStr,
    {
        let row = self.rows.get(row).ok_or(Error::RowNotFound)?;
        let column = row.get(column).ok_or(Error::ColumnNotFound)?;

        match column {
            Value::String(string) => string
                .split(pair_separator)
                .map(|i| {
                    let mut split = i.split(kv_separator);
                    let k: Option<K> = split.next().map(|k| k.parse().ok()).flatten();
                    let v: Option<V> = split.next().map(|v| v.parse().ok()).flatten();
                    k.zip(v)
                })
                .collect::<Option<_>>()
                .ok_or(Error::ValueConversionFailed),
            _ => Err(Error::InvalidColumnType),
        }
    }
}

#[test]
fn adding() {
    use std::io;

    // empty table
    let mut table = Table::new(1);
    let mut buffer = io::Cursor::new(Vec::new());
    table.serialize(&mut buffer).unwrap();
    assert_eq!(buffer.get_ref(), &[1, 0, 2, 0, 0, 0]);

    // row with invalid id
    let row = vec![Value::U8(0)];
    assert!(matches!(table.add_row(row), Err(Error::InvalidRowID)));

    // row with too many columns
    let mut row = vec![Value::I32(0)];
    for _ in 1..256 {
        row.push(Value::U8(0));
    }
    assert!(matches!(table.add_row(row), Err(Error::TooManyColumns)));

    // too many rows
    let mut table = Table::new(0);
    for _ in 0..65535 {
        table.add_row(vec![Value::I32(0)]).unwrap();
    }
    assert!(matches!(
        table.add_row(vec![Value::I32(0)]),
        Err(Error::TooManyRows)
    ));

    // inconsistent row length
    let mut table = Table::new(0);
    table.add_row(vec![Value::I32(0), Value::I32(0)]).unwrap();
    assert!(matches!(
        table.add_row(vec![Value::I32(0)]),
        Err(Error::InconsistentRowLength)
    ))
}

#[test]
fn getters() {
    let mut table = Table::new(1);
    table
        .add_row(vec![
            Value::I32(-1),
            Value::String("0,1,2".into()),
            Value::String("a:0,b:1,c:2".into()),
        ])
        .unwrap();

    assert!(matches!(table.value::<i32>(0, 0), Ok(-1)));
    assert!(matches!(table.value::<String>(0, 0).as_deref(), Ok("-1")));

    assert!(matches!(
        table.value::<i32>(0, 1),
        Err(Error::ValueConversionFailed)
    ));
    assert!(matches!(
        table.value::<String>(0, 1).as_deref(),
        Ok("0,1,2")
    ));
    assert!(matches!(
        table.vector::<i32>(0, 1, ",").as_deref(),
        Ok(&[0, 1, 2])
    ));

    let mut map = HashMap::new();
    map.insert("a".into(), 0);
    map.insert("b".into(), 1);
    map.insert("c".into(), 2);
    if let Ok(ret) = table.map::<String, i32>(0, 2, ",", ":") {
        assert!(ret == map);
    }
    assert!(matches!(table.value::<i32>(1, 0), Err(Error::RowNotFound)));

    use crate::{definitions::TableDefinition, NamedTable};
    let def = TableDefinition {
        name: "Test".into(),
        columns: vec!["id".into(), "array".into(), "map".into()],
        types: vec!["i32".into(), "string".into(), "string".into()],
    };
    let named = NamedTable::from_definition(table, &def);

    assert!(matches!(named.value::<i32>(-1, "id"), Ok(-1)));
    assert!(matches!(
        named.vector::<i32>(-1, "array", ",").as_deref(),
        Ok(&[0, 1, 2])
    ));
    if let Ok(ret) = named.map::<String, i32>(-1, "map", ",", ":") {
        assert!(ret == map);
    }
}

#[cfg(feature = "csv")]
#[test]
fn csv() {
    use std::io::BufRead;

    let csv = "id\ni32\n101";
    let mut file = std::io::Cursor::new(csv);

    let mut col_names = String::new();
    file.read_line(&mut col_names).unwrap();
    assert_eq!(col_names, "id\n");

    let table = Table::from_csv(5000, file).unwrap();
    assert_eq!(table.id, 5000);
    assert_eq!(table.rows, vec![vec![Value::I32(101)]]);

    let writer = table.to_csv(Vec::new(), false, true).unwrap();
    assert_eq!("i32\n101\n", String::from_utf8(writer).unwrap());
}
