use std::env;
use std::fs;
use std::path::PathBuf;

mod definitions;
mod stc;
mod catchdata;

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
    let delete = args.contains("-del");
    let definitions_path: Option<String> = args.opt_value_from_str("--def")?;
    let files = args.free()?;
    if files.is_empty() {
        log::info!("Usage: [files]");
        log::info!("Options:");
        log::info!("    --def    Path to table definitions");
        log::info!("     -del    Delete input file after processing");
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
                if let Err(why) = stc::to_csv(&path, &definitions) {
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
