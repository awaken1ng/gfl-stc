use std::{
    ffi::OsStr,
    fs,
    path::{Path, PathBuf},
};

use stc::definitions;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = pico_args::Arguments::from_env();
    let delete = args.contains("--del");
    let defs_path: Option<String> = args.opt_value_from_str("--def")?;
    let files = args.finish();
    if files.is_empty() {
        println!("Usage: [--def path] [--del] files");
        println!("Options:");
        println!("    --def    Path to table definitions to pull field names from");
        println!("    --del    Delete input file after processing");
        return Ok(());
    }

    let defs = match defs_path {
        Some(path) => {
            let contents = std::fs::read_to_string(path).expect("failed to read definitions file");
            stc::definitions::parse(&contents).expect("failed to parse definitions")
        }
        None => Default::default(),
    };

    for path in files.iter().map(PathBuf::from) {
        if !path.exists() || !path.is_file() {
            eprintln!("! Skipping: {}", path.display());
            continue;
        }

        match path.extension().map(OsStr::to_str).flatten() {
            Some("stc") => stc_to_csv(&path, &defs),
            _ => continue,
        }

        if delete {
            eprintln!("Deleting {}", path.display());
            fs::remove_file(path)?;
        }
    }

    Ok(())
}

fn stc_to_csv<P>(in_path: P, defs: &definitions::TableDefinitions)
where
    P: AsRef<Path>,
{
    let mut file = fs::File::open(&in_path).expect("failed to open stc file");
    let table = stc::table::Table::deserialize(&mut file).expect("failed to deserialize stc table");

    if table.records.len() == 0 {
        eprintln!("Table is empty: {}", in_path.as_ref().display());
        return;
    }

    let def = defs.get(&table.id);

    let out_path = match def {
        Some(def) => in_path
            .as_ref()
            .with_file_name(format!("{}_{}.csv", table.id, def.name)),
        None => in_path.as_ref().with_extension("csv"),
    };

    let n = out_path.file_name().unwrap_or_default().to_string_lossy();

    println!("Converting {} into {}", in_path.as_ref().display(), n);

    let mut writer = csv::WriterBuilder::default()
        .flexible(true)
        .from_path(out_path)
        .expect("failed to open file for writing");

    let (field_names, field_types): (Vec<String>, Vec<String>) = table
        .records
        .first()
        .unwrap() // SAFETY checked earlier
        .iter()
        .enumerate()
        .map(|(i, v)| {
            let field_name = def
                .map(|d| d.fields.get(i).map(ToOwned::to_owned))
                .flatten()
                .unwrap_or(format!("col_{}", i));

            (field_name, v.type_as_string())
        })
        .unzip();
    writer
        .write_record(&field_names)
        .expect("failed to write field names");
    writer
        .write_record(&field_types)
        .expect("failed to write field types");

    let bookmarks: Vec<String> = table.bookmarks.iter().map(|id| id.to_string()).collect();
    let bookmarks = format!("bookmarks:{}", bookmarks.join(","));
    writer
        .write_record(&[bookmarks])
        .expect("failed to write bookmarks");

    for record in table.records.iter() {
        let stringified = record.iter().map(|col| match col {
            stc::Value::String(string) => string.replace("\r", "\\r").replace("\n", "\\n"),
            other => other.to_string(),
        });
        writer
            .write_record(stringified)
            .expect("failed to write a record");
    }
}
