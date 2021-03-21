use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use std::{
    convert::{TryFrom, TryInto},
    io,
};

use crate::Error;

#[derive(Debug, Clone)]
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
                // UTF-8 is compatible with ASCII, so we can ignore this,
                // we could seek over it, but that would require io::Seek constraint on the reader
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

    pub fn serialize<W>(&self, writer: &mut W) -> Result<(), Error>
    where
        W: WriteBytesExt,
    {
        match self {
            Value::I8(v) => writer.write_i8(*v)?,
            Value::U8(v) => writer.write_u8(*v)?,
            Value::I16(v) => writer.write_i16::<LittleEndian>(*v)?,
            Value::U16(v) => writer.write_u16::<LittleEndian>(*v)?,
            Value::I32(v) => writer.write_i32::<LittleEndian>(*v)?,
            Value::U32(v) => writer.write_u32::<LittleEndian>(*v)?,
            Value::I64(v) => writer.write_i64::<LittleEndian>(*v)?,
            Value::U64(v) => writer.write_u64::<LittleEndian>(*v)?,
            Value::F32(v) => writer.write_f32::<LittleEndian>(*v)?,
            Value::F64(v) => writer.write_f64::<LittleEndian>(*v)?,
            Value::String(s) => {
                let is_ascii = s.is_ascii();
                writer.write_u8(is_ascii as u8)?;

                let len: u16 = s.len().try_into().map_err(|_| Error::StringTooBig)?;
                writer.write_u16::<LittleEndian>(len)?;

                writer.write_all(s.as_bytes())?;
            }
        }

        Ok(())
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
}

impl ToString for Value {
    fn to_string(&self) -> String {
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

#[derive(Debug)]
pub struct InvalidType;

macro_rules! impl_try_from {
    ($type:ty, $fn:tt) => {
        impl TryFrom<&Value> for $type {
            type Error = InvalidType;

            fn try_from(value: &Value) -> Result<Self, Self::Error> {
                value.$fn().ok_or(InvalidType)
            }
        }
    };
}

impl_try_from!(i8, as_i8);
impl_try_from!(u8, as_u8);
impl_try_from!(i16, as_i16);
impl_try_from!(u16, as_u16);
impl_try_from!(i32, as_i32);
impl_try_from!(u32, as_u32);
impl_try_from!(i64, as_i64);
impl_try_from!(u64, as_u64);
impl_try_from!(f32, as_f32);
impl_try_from!(f64, as_f64);

impl From<&Value> for String {
    fn from(v: &Value) -> Self {
        v.to_string()
    }
}
