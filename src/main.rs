use std::fs;
use std::path::PathBuf;
use std::{env, io, path::Path};

mod catchdata;
mod definitions;
mod stc;

use crate::definitions::TableDefinitions;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // initialize logger
    {
        let mut logger = pretty_env_logger::formatted_builder();
        if let Ok(config) = env::var("STC_LOG") {
            logger.parse_filters(&config);
        } else {
            logger.parse_filters("info");
        }
        logger.init();
    }

    // parse arguments
    let mut args = pico_args::Arguments::from_env();
    let delete = args.contains("--del");
    let definitions_path: Option<String> = args.opt_value_from_str("--def")?;
    let files = args.free()?;
    if files.is_empty() {
        log::info!("Usage: [files]");
        log::info!("Options:");
        log::info!("    --def    Path to table definitions");
        log::info!("    --del    Delete input file after processing");
        return Ok(());
    }

    let definitions = definitions::load(definitions_path)?;

    for path in files.iter().map(PathBuf::from) {
        if !path.exists() || !path.is_file() {
            continue;
        }

        match path.extension().unwrap().to_string_lossy().as_ref() {
            "stc" => {
                log::info!("Parsing {}", path.display());
                if let Err(why) = stc_to_csv(&path, &definitions) {
                    log::error!("Failed parsing {}: {}", path.display(), why)
                }
            }
            "dat" => {
                log::info!("Parsing {}", path.display());
                if let Err(why) = catchdata::parse(&path) {
                    log::error!("Failed parsing {}: {}", path.display(), why)
                }
            }
            _ => continue,
        }

        if delete {
            log::trace!("Deleting {}", path.display());
            fs::remove_file(path)?;
        }
    }

    Ok(())
}

pub(crate) fn stc_to_csv<P>(path: P, definitions: &TableDefinitions) -> io::Result<()>
where
    P: AsRef<Path>,
{
    let file = fs::File::open(&path)?;
    let table = stc::Table::read(file)?;

    if table.rows.len() == 0 {
        return Ok(());
    }

    let types: Vec<String> = table
        .rows
        .first()
        .unwrap() // SAFETY: checked above
        .iter()
        .map(stc::Value::type_as_string)
        .collect();

    let mut writer = match definitions.get(&table.id) {
        Some(def) => {
            if types != def.types {
                log::warn!("Field types in the file and in definitions are not matching");
                log::warn!("table={:?}", types);
                log::warn!("  def={:?}", def.types);
            }

            let out_path = path
                .as_ref()
                .with_file_name(format!("{}_{}.csv", table.id, def.name));
            let mut writer = csv::Writer::from_path(out_path)?;
            writer.write_record(&def.fields)?;
            writer
        }
        None => {
            log::warn!("No known field name definitions for {}", table.id);
            let out_path = path.as_ref().with_extension("csv");
            csv::Writer::from_path(out_path)?
        }
    };

    writer.write_record(types)?;

    for row in table.rows.iter() {
        writer.write_record(row.iter().map(|v| match v {
            stc::Value::String(s) => s.replace("\r", "\\r").replace("\n", "\\n"),
            _ => v.to_string(),
        }))?;
    }

    Ok(())
}
