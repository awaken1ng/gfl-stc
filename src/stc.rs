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
        unknown => unimplemented!("unimplemented type {}", unknown),
    }
}

pub(crate) fn to_csv<P>(path: P, definitions: &TableDefinitions) -> io::Result<()>
where
    P: AsRef<Path>,
{
    let mut reader = {
        let f = fs::File::open(&path).unwrap();
        BufReader::new(f)
    };

    let table_id = reader.read_u16::<LittleEndian>()?;
    log::debug!("table_id={}", table_id);
    let unknown1 = reader.read_u16::<LittleEndian>()?;
    log::debug!("unknown1={}", unknown1);
    let rows = reader.read_u16::<LittleEndian>()?;
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

    let unknown2 = reader.read_u32::<LittleEndian>()?;
    log::debug!("unknown2={}", unknown2);
    let rows_offset = reader.read_u32::<LittleEndian>()?;
    log::debug!("rows_offset={}", rows_offset);
    let unknown3 = {
        let len = rows_offset - reader.seek(SeekFrom::Current(0))? as u32;
        let mut buffer = vec![0; len as usize];
        reader.read_exact(&mut buffer)?;
        buffer
    };
    log::debug!("unknown3.len={}", unknown3.len());

    let path = match definitions.get(&table_id) {
        Some(definition) => path
            .as_ref()
            .with_file_name(format!("{}_{}.csv", table_id, definition.name)),
        None => path.as_ref().with_extension("csv"),
    };
    let mut writer = csv::Writer::from_path(path)?;

    // write header
    match definitions.get(&table_id) {
        Some(definition) => {
            log::debug!("Writing field names");
            writer.write_record(&definition.fields)?;
        }
        None => log::warn!("No known field name definitions for {}", table_id),
    };
    log::debug!("Writing field types");
    writer.write_record(field_types.iter().map(type_to_str))?;

    reader.seek(SeekFrom::Start(u64::from(rows_offset)))?;
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
                    log::trace!("Reading string");

                    let unknown = reader.read_u8()?;
                    log::trace!("| unknown={}", unknown);
                    let len = reader.read_u16::<LittleEndian>()?;
                    log::trace!("| len={}", len);

                    let mut v = vec![0; usize::from(len)];
                    reader.read_exact(&mut v)?;
                    String::from_utf8_lossy(&v).to_string()
                }
                unknown => unimplemented!("unimplemented type {}", unknown)
            };
            row.push_field(&v);
        }
        log::trace!("Writing row");
        writer.write_record(row.into_iter())?;
    }

    Ok(())
}
