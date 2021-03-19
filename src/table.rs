use std::{
    collections::HashMap,
    convert::{TryFrom, TryInto},
    hash::Hash,
    io::{Read, Seek, SeekFrom},
    str::FromStr,
};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use crate::{AccessError, ParsingError, Value};

pub type Record = Vec<Value>;

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

    pub fn deserialize<R>(reader: &mut R) -> Result<Self, ParsingError>
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
            return Err(ParsingError::LastBlockSizeMismatch);
        }

        Ok(table)
    }

    pub fn add_record(&mut self, record: Vec<Value>) -> Result<(), ParsingError> {
        if self.records.len() >= u16::MAX.into() {
            return Err(ParsingError::TableIsFull);
        }

        if record.len() > u8::MAX.into() {
            return Err(ParsingError::TooManyFields);
        }

        // first value must be i32
        match record.first() {
            Some(Value::I32(_)) => {}
            _ => return Err(ParsingError::InvalidID),
        }

        if let Some(first) = self.records.first() {
            if first.len() != record.len() {
                return Err(ParsingError::InconsistentLength);
            }
        }

        self.records.push(record);

        Ok(())
    }

    pub fn serialize<W>(&self, writer: &mut W) -> Result<(), ParsingError>
    where
        W: WriteBytesExt + Seek,
    {
        writer.write_u16::<LittleEndian>(self.id)?;

        writer.write_u16::<LittleEndian>(2)?; // lbs placeholder, current position

        let records_n = self
            .records
            .len()
            .try_into()
            .map_err(|_| ParsingError::TableIsFull)?;

        writer.write_u16::<LittleEndian>(records_n)?;

        if self.records.is_empty() {
            return Ok(());
        }

        // SAFETY checked above
        let first = self.records.first().unwrap();

        let fields_n: u8 = first
            .len()
            .try_into()
            .map_err(|_| ParsingError::TooManyFields)?;
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
                    let id = field.as_i32().ok_or(ParsingError::InvalidID)?;
                    let pos: u32 = writer
                        .seek(SeekFrom::Current(0))?
                        .try_into()
                        .map_err(|_| ParsingError::OutOfBounds)?;

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

    pub fn value<'a, T>(&'a self, row: usize, column: usize) -> Result<T, AccessError>
    where
        T: TryFrom<&'a Value>,
    {
        let row = self.records.get(row).ok_or(AccessError::RowNotFound)?;
        let column = row.get(column).ok_or(AccessError::ColumnNotFound)?;

        T::try_from(column).map_err(|_| AccessError::ConversionFailed)
    }

    /// Convert `"v,v,v"` string into `Vec<T>`
    pub fn array<'a, T>(
        &'a self,
        row: usize,
        column: usize,
        separator: &str,
    ) -> Result<Vec<T>, AccessError>
    where
        T: FromStr,
    {
        let row = self.records.get(row).ok_or(AccessError::RowNotFound)?;
        let column = row.get(column).ok_or(AccessError::ColumnNotFound)?;

        match column {
            Value::String(string) => string
                .split(separator)
                .map(T::from_str)
                .collect::<Result<Vec<T>, _>>()
                .map_err(|_| AccessError::ConversionFailed),
            _ => Err(AccessError::UnexpectedType),
        }
    }

    pub fn map<K, V>(
        &self,
        row: usize,
        column: usize,
        pair_separator: &str,
        kv_separator: &str,
    ) -> Result<HashMap<K, V>, AccessError>
    where
        K: FromStr + Eq + Hash,
        V: FromStr,
    {
        let row = self.records.get(row).ok_or(AccessError::RowNotFound)?;
        let column = row.get(column).ok_or(AccessError::ColumnNotFound)?;

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
                .ok_or(AccessError::ConversionFailed),
            _ => Err(AccessError::UnexpectedType),
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

    // record with invalid id
    let record = vec![Value::U8(0)];
    assert!(matches!(
        table.add_record(record),
        Err(ParsingError::InvalidID)
    ));

    // record with too many fields
    let mut record = vec![Value::I32(0)];
    for _ in 1..256 {
        record.push(Value::U8(0));
    }
    assert!(matches!(
        table.add_record(record),
        Err(ParsingError::TooManyFields)
    ));

    // too many rows
    let mut table = Table::new(0);
    for _ in 0..65535 {
        table.add_record(vec![Value::I32(0)]).unwrap();
    }
    assert!(matches!(
        table.add_record(vec![Value::I32(0)]),
        Err(ParsingError::TableIsFull)
    ));

    // inconsistent row length
    let mut table = Table::new(0);
    table
        .add_record(vec![Value::I32(0), Value::I32(0)])
        .unwrap();
    assert!(matches!(
        table.add_record(vec![Value::I32(0)]),
        Err(ParsingError::InconsistentLength)
    ))
}

#[test]
fn getters() {
    let mut table = Table::new(1);
    table
        .add_record(vec![
            Value::I32(-1),
            Value::String("0,1,2".into()),
            Value::String("a:0,b:1,c:2".into()),
        ])
        .unwrap();

    assert_eq!(table.value::<i32>(0, 0), Ok(-1));
    assert_eq!(table.value::<String>(0, 0), Ok("-1".into()));

    let array = vec![0, 1, 2];
    assert_eq!(table.value::<i32>(0, 1), Err(AccessError::ConversionFailed));
    assert_eq!(table.value::<String>(0, 1), Ok("0,1,2".into()));
    assert_eq!(table.array::<i32>(0, 1, ",").as_ref(), Ok(&array));

    let mut map = HashMap::new();
    map.insert("a".into(), 0);
    map.insert("b".into(), 1);
    map.insert("c".into(), 2);
    assert_eq!(table.map::<String, i32>(0, 2, ",", ":").as_ref(), Ok(&map));

    assert_eq!(table.value::<i32>(1, 0), Err(AccessError::RowNotFound));

    use crate::{definitions::TableDefinition, NamedTable};
    let def = TableDefinition {
        name: "Test".into(),
        fields: vec!["id".into(), "array".into(), "map".into()],
        types: vec!["i32".into(), "string".into(), "string".into()],
    };
    let named = NamedTable::from_definition(table, &def);

    assert_eq!(named.value::<i32>(-1, "id"), Ok(-1));
    assert_eq!(named.vector::<i32>(-1, "array", ","), Ok(array));
    assert_eq!(named.map::<String, i32>(-1, "map", ",", ":"), Ok(map));
}
