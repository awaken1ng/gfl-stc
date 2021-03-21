use std::{
    ffi::OsStr,
    fmt::Display,
    fs,
    io::Write,
    path::{Path, PathBuf},
};

use termcolor::{Color, ColorChoice, ColorSpec, StandardStream, WriteColor};

use stc::definitions;

fn colored_println<D>(prefix: &str, color: termcolor::Color, message: D)
where
    D: Display,
{
    let mut stdout = StandardStream::stdout(ColorChoice::Auto);
    stdout
        .set_color(ColorSpec::new().set_fg(Some(color)).set_bold(true))
        .expect("failed to set text colour");
    write!(&mut stdout, "{} ", prefix).expect("failed to write to stdout");

    stdout.reset().expect("failed to reset text color");
    writeln!(&mut stdout, "{}", message).expect("failed to write to stdout");
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = pico_args::Arguments::from_env();
    let delete = args.contains("--del");
    let defs_path: Option<String> = args.opt_value_from_str("--def")?;
    let files = args.finish();
    if files.is_empty() {
        println!("Usage: [--def path] [--del] files");
        println!("Options:");
        println!("    --def    Path to table definitions to pull column names from");
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
            colored_println("Skipping", Color::Yellow, path.display());
            continue;
        }

        match path.extension().map(OsStr::to_str).flatten() {
            Some("stc") => stc_to_csv(&path, &defs),
            _ => continue,
        }

        if delete {
            colored_println("Deleting", Color::Red, path.display());
            fs::remove_file(path)?;
        }
    }

    Ok(())
}

fn stc_to_csv<P>(in_path: P, defs: &definitions::TableDefinitions)
where
    P: AsRef<Path>,
{
    let in_path = in_path.as_ref();
    let mut file = fs::File::open(&in_path).expect("failed to open stc file");
    let table = stc::Table::deserialize(&mut file).expect("failed to deserialize stc table");

    if table.rows.is_empty() {
        colored_println("   Empty", Color::Cyan, in_path.display());
        return;
    }

    let def = defs.get(&table.id);

    let out_path = match def {
        Some(def) => in_path.with_file_name(format!("{}_{}.csv", table.id, def.name)),
        None => in_path.with_extension("csv"),
    };

    colored_println(" Parsing", Color::Green, in_path.display());

    let mut writer = csv::WriterBuilder::default()
        .from_path(out_path)
        .expect("failed to open file for writing");

    let (column_names, column_types): (Vec<String>, Vec<String>) = table
        .rows
        .first()
        .unwrap() // SAFETY checked earlier
        .iter()
        .enumerate()
        .map(|(i, v)| {
            let column_name = def
                .map(|d| d.columns.get(i).map(ToOwned::to_owned))
                .flatten()
                .unwrap_or(format!("col_{}", i));

            (column_name, v.type_as_string())
        })
        .unzip();
    writer
        .write_record(&column_names)
        .expect("failed to write column names");
    writer
        .write_record(&column_types)
        .expect("failed to write column types");

    for row in table.rows.iter() {
        let stringified = row.iter().map(|col| match col {
            stc::Value::String(string) => string.replace("\r", "\\r").replace("\n", "\\n"),
            other => other.to_string(),
        });
        writer
            .write_record(stringified)
            .expect("failed to write a row");
    }
}
