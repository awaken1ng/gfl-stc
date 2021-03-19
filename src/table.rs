use std::{
    collections::HashMap,
    convert::{TryFrom, TryInto},
    hash::Hash,
    io::{self, Read, Seek, SeekFrom},
    str::FromStr,
};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use crate::{definitions::TableDefinition, Value};

pub type Record = Vec<Value>;

#[derive(Debug)]
pub enum Error {
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

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Self::IO(err)
    }
}

#[derive(Debug, Clone)]
pub struct Table {
    pub id: u16,
    pub records: Vec<Record>,
}

impl Table {
    pub fn new(id: u16) -> Self {
        Self {
            id,
            records: Vec::new(),
        }
    }

    pub fn deserialize<R>(reader: &mut R) -> io::Result<Self>
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

        let fields: usize = reader.read_u8()?.into();
        let mut field_types = Vec::with_capacity(fields);
        for _ in 0..fields {
            let t = reader.read_u8()?;
            field_types.push(t);
        }

        // read jump table
        let _first_row_id = reader.read_i32::<LittleEndian>()?;
        let first_row_offset: u64 = reader.read_u32::<LittleEndian>()?.into();

        // skip the rest of the table
        reader.seek(SeekFrom::Start(first_row_offset))?;

        for _ in 0..rows {
            let mut row = Vec::with_capacity(fields);

            for t in &field_types {
                row.push(Value::read(*t, reader)?);
            }

            table.records.push(row);
        }

        let cur_pos = reader.seek(SeekFrom::Current(0))?;
        if last_block_size != (cur_pos - 4) % 65536 {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "last block sizes didn't match",
            ));
        }

        Ok(table)
    }

    pub fn add_record(&mut self, record: Vec<Value>) -> Result<(), Error> {
        if self.records.len() >= u16::MAX.into() {
            return Err(Error::TableIsFull);
        }

        if record.len() > u8::MAX.into() {
            return Err(Error::TooManyFields);
        }

        // first value must be i32
        match record.first() {
            Some(Value::I32(_)) => {}
            _ => return Err(Error::InvalidID),
        }

        if let Some(first) = self.records.first() {
            if first.len() != record.len() {
                return Err(Error::InconsistentLength);
            }
        }

        self.records.push(record);

        Ok(())
    }

    pub fn serialize<W>(&self, writer: &mut W) -> Result<(), Error>
    where
        W: WriteBytesExt + Seek,
    {
        writer.write_u16::<LittleEndian>(self.id)?;

        writer.write_u16::<LittleEndian>(2)?; // lbs placeholder, current position

        let records_n = self
            .records
            .len()
            .try_into()
            .map_err(|_| Error::TableIsFull)?;

        writer.write_u16::<LittleEndian>(records_n)?;

        if self.records.is_empty() {
            return Ok(());
        }

        // SAFETY checked above
        let first = self.records.first().unwrap();

        let fields_n: u8 = first.len().try_into().map_err(|_| Error::TooManyFields)?;
        writer.write_u8(fields_n)?;

        // field types
        for v in first.iter() {
            writer.write_u8(v.type_as_u8())?;
        }

        // jump table placeholder
        let jump_table_size = 1 + (self.records.len() / 100);
        for _ in 0..jump_table_size {
            writer.write_i32::<LittleEndian>(0)?; // id
            writer.write_u32::<LittleEndian>(0)?; // offset
        }

        let mut jump_table = Vec::with_capacity(jump_table_size);

        for (row_i, row) in self.records.iter().enumerate() {
            for (field_i, field) in row.into_iter().enumerate() {
                if row_i % 100 == 0 && field_i == 0 {
                    let id = field.as_i32().ok_or(Error::InvalidID)?;
                    let pos: u32 = writer
                        .seek(SeekFrom::Current(0))?
                        .try_into()
                        .map_err(|_| Error::OutOfBounds)?;

                    jump_table.push((id, pos));
                }

                field.serialize(writer)?;
            }
        }

        let lbs = (writer.seek(SeekFrom::Current(0))? - 4) % 65536;
        writer.seek(SeekFrom::Start(2))?;
        writer.write_u16::<LittleEndian>(lbs as u16)?;

        assert_eq!(jump_table.len(), jump_table_size);

        // seek to the start of the jump table
        // id (2), lbs (2), rows_n (2), fields_n (1), field_types (fields_n)
        writer.seek(SeekFrom::Start(7 + u64::from(fields_n)))?;
        for (id, offset) in jump_table {
            writer.write_i32::<LittleEndian>(id)?;
            writer.write_u32::<LittleEndian>(offset)?;
        }

        writer.flush()?;

        Ok(())
    }

    pub fn value<'a, T>(&'a self, row: usize, column: usize) -> Option<T>
    where
        T: TryFrom<&'a Value>,
    {
        let field = self.records.get(row)?.get(column)?;
        T::try_from(field).ok()
    }

    /// Convert `"v,v,v"` string into `Vec<T>`
    pub fn array<'a, T>(&'a self, row: usize, column: usize, separator: &str) -> Option<Vec<T>>
    where
        T: FromStr,
    {
        match self.records.get(row)?.get(column)? {
            Value::String(string) => string
                .split(separator)
                .map(T::from_str)
                .collect::<Result<Vec<T>, _>>()
                .ok(),
            _ => None,
        }
    }

    pub fn map<K, V>(
        &self,
        row: usize,
        column: usize,
        pair_separator: &str,
        kv_separator: &str,
    ) -> Option<HashMap<K, V>>
    where
        K: FromStr + Eq + Hash,
        V: FromStr,
    {
        match self.records.get(row)?.get(column)? {
            Value::String(string) => string
                .split(pair_separator)
                .map(|i| {
                    let mut split = i.split(kv_separator);
                    let k: Option<K> = split.next().map(|k| k.parse().ok()).flatten();
                    let v: Option<V> = split.next().map(|v| v.parse().ok()).flatten();
                    k.zip(v)
                })
                .collect(),
            _ => None,
        }
    }
}

pub struct NamedTable {
    pub name: String,
    pub fields: Vec<String>,
    pub table: Table,
}

impl NamedTable {
    pub fn from_definition(table: Table, def: &TableDefinition) -> Self {
        Self {
            name: def.name.clone(),
            fields: def.fields.clone(),
            table,
        }
    }

    pub fn value<'a, T>(&'a self, row: usize, column: &str) -> Option<T>
    where
        T: TryFrom<&'a Value>,
    {
        let column = self.fields.iter().position(|n| n == column)?;
        self.table.value(row, column)
    }

    pub fn array<'a, T>(&'a self, row: usize, column: &str, separator: &str) -> Option<Vec<T>>
    where
        T: FromStr,
    {
        let column = self.fields.iter().position(|n| n == column)?;
        self.table.array(row, column, separator)
    }

    pub fn map<K, V>(
        &self,
        row: usize,
        column: &str,
        pair_separator: &str,
        kv_separator: &str,
    ) -> Option<HashMap<K, V>>
    where
        K: FromStr + Eq + Hash,
        V: FromStr,
    {
        let column = self.fields.iter().position(|n| n == column)?;
        self.table.map(row, column, pair_separator, kv_separator)
    }
}

#[test]
fn adding() {
    // empty table
    let mut table = Table::new(1);
    let mut buffer = io::Cursor::new(Vec::new());
    table.serialize(&mut buffer).unwrap();
    assert_eq!(buffer.get_ref(), &[1, 0, 2, 0, 0, 0]);

    // record with invalid id
    let record = vec![Value::U8(0)];
    assert!(matches!(table.add_record(record), Err(Error::InvalidID)));

    // record with too many fields
    let mut record = vec![Value::I32(0)];
    for _ in 1..256 {
        record.push(Value::U8(0));
    }
    assert!(matches!(
        table.add_record(record),
        Err(Error::TooManyFields)
    ));

    // too many rows
    let mut table = Table::new(0);
    for _ in 0..65535 {
        table.add_record(vec![Value::I32(0)]).unwrap();
    }
    assert!(matches!(
        table.add_record(vec![Value::I32(0)]),
        Err(Error::TableIsFull)
    ));

    // inconsistent row length
    let mut table = Table::new(0);
    table
        .add_record(vec![Value::I32(0), Value::I32(0)])
        .unwrap();
    assert!(matches!(
        table.add_record(vec![Value::I32(0)]),
        Err(Error::InconsistentLength)
    ))
}

#[test]
fn getters() {
    let mut table = Table::new(1);
    table
        .add_record(vec![
            Value::I32(0),
            Value::String("0,1,2".into()),
            Value::String("a:0,b:1,c:2".into()),
        ])
        .unwrap();

    assert_eq!(table.value::<i32>(0, 0), Some(0));
    assert_eq!(table.value::<String>(0, 0), Some("0".into()));

    let array = vec![0, 1, 2];
    assert_eq!(table.value::<i32>(0, 1), None);
    assert_eq!(table.value::<String>(0, 1), Some("0,1,2".into()));
    assert_eq!(table.array::<i32>(0, 1, ",").as_ref(), Some(&array));

    let mut map = HashMap::new();
    map.insert("a".into(), 0);
    map.insert("b".into(), 1);
    map.insert("c".into(), 2);
    assert_eq!(
        table.map::<String, i32>(0, 2, ",", ":").as_ref(),
        Some(&map)
    );

    assert_eq!(table.value::<i32>(1, 0), None);

    let named = NamedTable {
        name: "Test".into(),
        fields: vec!["id".into(), "array".into(), "map".into()],
        table,
    };

    assert_eq!(named.value::<i32>(0, "id"), Some(0));
    assert_eq!(named.array::<i32>(0, "array", ","), Some(array));
    assert_eq!(named.map::<String, i32>(0, "map", ",", ":"), Some(map));
}
