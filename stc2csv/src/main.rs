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
    let mut table = stc::Table::deserialize(&mut file).expect("failed to deserialize stc table");

    let def = defs.get(&table.id);

    let out_path = match def {
        Some(def) => in_path.with_file_name(format!("{}_{}.csv", table.id, def.name)),
        None => in_path.with_extension("csv"),
    };

    if table.rows.is_empty() {
        colored_println("   Empty", Color::Cyan, in_path.display());

        // `to_csv` doesn't write headers if table is empty
        if let Some(def) = def {
            let mut out = csv::Writer::from_path(out_path).unwrap();

            out.write_record(&def.columns).expect("failed to write column names");
            out.write_record(&def.types).expect("failed to write column types");
            out.flush().expect("failed to flush");
        }

        return;
    }

    colored_println(" Parsing", Color::Green, in_path.display());

    // escape new lines
    for row in table.rows.iter_mut() {
        for col in row.iter_mut() {
            if let stc::Value::String(string) = col {
                let escaped = string.replace("\r", "\\r").replace("\n", "\\n");
                *string = escaped;
            }
        }
    }

    let out = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(out_path)
        .expect("failed to open file for writing");

    match def {
        Some(def) => stc::NamedTable::from_definition(table, def)
            .expect("failed to create named table")
            .to_csv(out, true, true),
        None => table.to_csv(out, true, true),
    }
    .expect("failed to convert to csv");
}
