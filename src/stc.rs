use byteorder::{LittleEndian, ReadBytesExt};

use std::io::{self, BufReader, Read, Seek, SeekFrom};

#[derive(Debug)]
pub enum Value {
    I8(i8),
    U8(u8),
    I16(i16),
    U16(u16),
    I32(i32),
    U32(u32),
    I64(i64),
    U64(u64),
    F32(f32),
    F64(f64),
    String(String),
}

macro_rules! impl_as {
    ($item:tt, $name:ident -> $type:ty) => {
        pub fn $name(&self) -> Option<$type> {
            match self {
                Value::$item(v) => Some(*v),
                _ => None,
            }
        }
    };
}

impl Value {
    pub fn read<R>(field_type: u8, reader: &mut R) -> io::Result<Value>
    where
        R: ReadBytesExt,
    {
        let value = match field_type {
            1 => Value::I8(reader.read_i8()?),
            2 => Value::U8(reader.read_u8()?),
            3 => Value::I16(reader.read_i16::<LittleEndian>()?),
            4 => Value::U16(reader.read_u16::<LittleEndian>()?),
            5 => Value::I32(reader.read_i32::<LittleEndian>()?),
            6 => Value::U32(reader.read_u32::<LittleEndian>()?),
            7 => Value::I64(reader.read_i64::<LittleEndian>()?),
            8 => Value::U64(reader.read_u64::<LittleEndian>()?),
            9 => Value::F32(reader.read_f32::<LittleEndian>()?),
            10 => Value::F64(reader.read_f64::<LittleEndian>()?),
            11 => {
                // UTF-8 is compatible with ASCII, so we can ignore this
                // we could seek over it, but that would require io::Seek requirement on the reader
                reader.read_u8()?; // step over `is_ascii` flag

                let len = reader.read_u16::<LittleEndian>()?;
                let mut buffer = vec![0; usize::from(len)];
                reader.read_exact(&mut buffer)?;

                let string = String::from_utf8_lossy(&buffer).to_string();
                Value::String(string)
            }
            unknown => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unknown value type {}", unknown),
                ))
            }
        };

        Ok(value)
    }

    pub fn type_as_u8(&self) -> u8 {
        match self {
            Value::I8(_) => 1,
            Value::U8(_) => 2,
            Value::I16(_) => 3,
            Value::U16(_) => 4,
            Value::I32(_) => 5,
            Value::U32(_) => 6,
            Value::I64(_) => 7,
            Value::U64(_) => 8,
            Value::F32(_) => 9,
            Value::F64(_) => 10,
            Value::String(_) => 11,
        }
    }
    pub fn type_as_string(&self) -> String {
        match self {
            Value::I8(_) => "i8",
            Value::U8(_) => "u8",
            Value::I16(_) => "i16",
            Value::U16(_) => "u16",
            Value::I32(_) => "i32",
            Value::U32(_) => "u32",
            Value::I64(_) => "i64",
            Value::U64(_) => "u64",
            Value::F32(_) => "f32",
            Value::F64(_) => "f64",
            Value::String(_) => "string",
        }
        .to_string()
    }
    impl_as!(I8, as_i8 -> i8);
    impl_as!(U8, as_u8 -> u8);
    impl_as!(I16, as_i16 -> i16);
    impl_as!(U16, as_u16 -> u16);
    impl_as!(I32, as_i32 -> i32);
    impl_as!(U32, as_u32 -> u32);
    impl_as!(I64, as_i64 -> i64);
    impl_as!(U64, as_u64 -> u64);
    impl_as!(F32, as_f32 -> f32);
    impl_as!(F64, as_f64 -> f64);
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::String(v) => Some(v),
            _ => None,
        }
    }
    pub fn to_string(&self) -> String {
        match self {
            Value::I8(v) => v.to_string(),
            Value::U8(v) => v.to_string(),
            Value::I16(v) => v.to_string(),
            Value::U16(v) => v.to_string(),
            Value::I32(v) => v.to_string(),
            Value::U32(v) => v.to_string(),
            Value::I64(v) => v.to_string(),
            Value::U64(v) => v.to_string(),
            Value::F32(v) => v.to_string(),
            Value::F64(v) => v.to_string(),
            Value::String(v) => v.to_string(),
        }
    }
}

pub type Record = Vec<Value>;

pub struct Table {
    pub id: u16,
    jump_table: Vec<i32>, // contains record ids, must always have the id of first record
    pub rows: Vec<Record>,
}

impl Table {
    pub fn read<R>(data: R) -> io::Result<Self>
    where
        R: Read + Seek,
    {
        let mut reader = BufReader::new(data);

        let table_id = reader.read_u16::<LittleEndian>()?;
        let last_block_size: u64 = reader.read_u16::<LittleEndian>()?.into(); // size of the last 65kb block
        let rows = reader.read_u16::<LittleEndian>()?;

        let mut table = Self {
            id: table_id,
            jump_table: Vec::default(),
            rows: Vec::default(),
        };

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
        let first_row_id = reader.read_i32::<LittleEndian>()?;
        let first_row_offset: u64 = reader.read_u32::<LittleEndian>()?.into();
        table.jump_table.push(first_row_id);

        loop {
            let cur_pos = reader.seek(SeekFrom::Current(0))?;
            if cur_pos == first_row_offset {
                break; // reached the end of the table
            }

            let id = reader.read_i32::<LittleEndian>()?;
            reader.seek(SeekFrom::Current(4))?; // skip offset
            table.jump_table.push(id);
        }

        for _ in 0..rows {
            let mut row = Vec::with_capacity(fields);

            for t in &field_types {
                row.push(Value::read(*t, &mut reader)?);
            }

            table.rows.push(row);
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
}
