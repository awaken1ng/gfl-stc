use byteorder::{LittleEndian, ReadBytesExt};
use csv::StringRecord;

use std::fs;
use std::io::{self, BufReader, Read, Seek, SeekFrom};
use std::path::Path;

use crate::definitions::TableDefinitions;

fn type_to_str<'a>(t: &u8) -> &'a str {
    match t {
        1 => "i8",
        2 => "u8",
        3 => "i16",
        4 => "u16",
        5 => "i32",
        6 => "u32",
        7 => "i64",
        8 => "u64",
        9 => "f32",
        10 => "f64",
        11 => "string",
        unknown => unimplemented!("{}", unknown),
    }
}

pub(crate) fn to_csv<P>(path: P, definitions: &TableDefinitions) -> io::Result<()>
where
    P: AsRef<Path>,
{
    let mut reader = {
        let file = fs::File::open(&path)?;
        BufReader::new(file)
    };

    let table_id = reader.read_u16::<LittleEndian>()?;
    let last_block_size = reader.read_u16::<LittleEndian>()?; // size of the last 65kb block
    let rows = reader.read_u16::<LittleEndian>()?;
    log::debug!("table_id={}", table_id);
    log::debug!("last_block_size={}", last_block_size);
    log::debug!("rows={}", rows);

    if rows == 0 {
        return Ok(());
    }

    // read type table
    let fields = reader.read_u8()?;
    log::debug!("fields={}", fields);

    let mut field_types = Vec::with_capacity(usize::from(fields));
    for _ in 0..fields {
        let t = reader.read_u8()?;
        field_types.push(t);
    }
    log::debug!("field_types.raw={:?}", field_types);
    log::debug!(
        "field_types.mapped={}",
        field_types
            .iter()
            .map(type_to_str)
            .collect::<Vec<&str>>()
            .join(",")
    );

    // read jump table
    reader.seek(SeekFrom::Current(4))?; // step over first row id
    let first_row_offset = reader.read_u32::<LittleEndian>()?;
    // skip the rest of the table
    reader.seek(SeekFrom::Start(u64::from(first_row_offset)))?;
    log::debug!("first_row_offset={}", first_row_offset);

    let mut writer = match definitions.get(&table_id) {
        Some(def) => {
            log::debug!("Writing field names");
            let out_path = path
                .as_ref()
                .with_file_name(format!("{}_{}.csv", table_id, def.name));
            let mut writer = csv::Writer::from_path(out_path)?;
            writer.write_record(&def.fields)?;
            writer
        }
        None => {
            log::warn!("No known field name definitions for {}", table_id);
            let out_path = path.as_ref().with_extension("csv");
            csv::Writer::from_path(out_path)?
        }
    };

    log::debug!("Writing field types");
    writer.write_record(field_types.iter().map(type_to_str))?;

    log::debug!("Reading data");
    for _ in 0..rows {
        let mut row = StringRecord::new();

        for field_type in &field_types {
            let v = match field_type {
                1 => reader.read_i8()?.to_string(),
                2 => reader.read_u8()?.to_string(),
                3 => reader.read_i16::<LittleEndian>()?.to_string(),
                4 => reader.read_u16::<LittleEndian>()?.to_string(),
                5 => reader.read_i32::<LittleEndian>()?.to_string(),
                6 => reader.read_u32::<LittleEndian>()?.to_string(),
                7 => reader.read_i64::<LittleEndian>()?.to_string(),
                8 => reader.read_u64::<LittleEndian>()?.to_string(),
                9 => reader.read_f32::<LittleEndian>()?.to_string(),
                10 => reader.read_f64::<LittleEndian>()?.to_string(),
                11 => {
                    reader.seek(SeekFrom::Current(1))?; // step over `is_ascii` flag

                    let len = reader.read_u16::<LittleEndian>()?;
                    let mut buffer = vec![0; usize::from(len)];
                    reader.read_exact(&mut buffer)?;

                    String::from_utf8_lossy(&buffer).to_string()
                }
                unknown => unimplemented!("type {}", unknown),
            };
            row.push_field(&v);
        }
        writer.write_record(row.into_iter())?;
    }

    let cur_pos = reader.seek(SeekFrom::Current(0))?;
    if u64::from(last_block_size) != (cur_pos - 4) % 65536 {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            "last block sizes didn't match",
        ));
    }

    Ok(())
}
