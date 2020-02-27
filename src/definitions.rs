use std::collections::HashMap;
use std::fs;
use std::io::{self, BufRead, Cursor};

#[derive(Debug)]
pub(crate) struct TableDefinition {
    pub(crate) name: String,
    pub(crate) fields: Vec<String>,
}

pub(crate) type TableDefinitions = HashMap<u16, TableDefinition>;

pub(crate) fn load(path: Option<String>) -> io::Result<TableDefinitions> {
    let path = match path {
        Some(path) => path,
        None => return Ok(HashMap::default()),
    };
    let file = fs::read_to_string(path)?;
    let mut buffer = Cursor::new(file);

    // read first line "{region},{version}"
    let region = {
        let mut region = Vec::default();
        buffer.read_until(b',', &mut region)?;
        String::from_utf8_lossy(&region)
            .trim_end_matches(',')
            .to_string()
    };
    let version = {
        let mut version = String::default();
        buffer.read_line(&mut version)?;
        version.trim().to_string()
    };
    log::info!(
        "Reading table definitions from {} client v{}",
        region,
        version
    );

    let mut definitions = HashMap::new();

    for line in buffer.lines() {
        let line = line?;
        let mut line: Vec<&str> = line.split(',').collect();

        let id: u16 = line
            .remove(0)
            .parse()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        let name = line.remove(0).to_string();
        let fields: Vec<String> = line.into_iter().map(String::from).collect();

        definitions.insert(id, TableDefinition { name, fields });
    }

    log::info!("| {} definitions were read", definitions.len());
    Ok(definitions)
}
