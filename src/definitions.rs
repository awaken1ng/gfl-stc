use std::collections::HashMap;
use std::fs;
use std::io::{self, BufRead, Cursor};

#[derive(Debug)]
pub(crate) struct TableDefinition {
    pub(crate) name: String,
    pub(crate) fields: Vec<String>,
    pub(crate) types: Vec<String>,
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
    let header = {
        let mut string = String::default();
        buffer.read_line(&mut string)?;
        string
    };
    let mut header = header.trim().split(',');

    let region = header.next().unwrap();
    let version = header.next().unwrap();

    log::info!(
        "Reading table definitions from {} client v{}",
        region,
        version
    );

    let mut definitions = HashMap::new();

    for line in buffer.lines() {
        let line = line?;
        let mut line: Vec<&str> = line.split(';').collect();

        let id: u16 = line
            .remove(0)
            .parse()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        let name = line.remove(0).to_string();
        let fields = line.remove(0).split(',').map(String::from).collect();
        let types = line.remove(0).split(',').map(String::from).collect();

        definitions.insert(id, TableDefinition { name, fields, types });
    }

    log::info!("| {} definitions were read", definitions.len());
    Ok(definitions)
}
