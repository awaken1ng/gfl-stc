use std::collections::HashMap;

#[derive(Debug, PartialEq)]
pub struct TableDefinition {
    pub name: String,
    pub fields: Vec<String>,
    pub types: Vec<String>,
}

pub type TableDefinitions = HashMap<u16, TableDefinition>;

#[derive(Debug)]
pub enum Error {
    NoID,
    InvalidID(std::num::ParseIntError),
    NoName,
    NoFieldNames,
    NoFieldTypes,
    FieldNamesAndTypesMismatch,
}

pub fn parse(
    // path: Option<String>
    contents: &str,
) -> Result<TableDefinitions, Error> {
    let mut definitions = HashMap::new();

    for line in contents.lines() {
        let line = line.trim();
        if line.starts_with("//") || line.is_empty() {
            continue;
        }

        let mut line = line.split(";");
        let id = line
            .next()
            .ok_or(Error::NoID)?
            .parse()
            .map_err(Error::InvalidID)?;
        let name = line.next().ok_or(Error::NoName)?.to_owned();
        let fields: Vec<String> = line
            .next()
            .ok_or(Error::NoFieldNames)?
            .split(",")
            .map(String::from)
            .collect();
        let types: Vec<String> = line
            .next()
            .ok_or(Error::NoFieldTypes)?
            .split(",")
            .map(String::from)
            .collect();

        if fields.len() != types.len() {
            return Err(Error::FieldNamesAndTypesMismatch);
        }

        definitions.insert(
            id,
            TableDefinition {
                name,
                fields,
                types,
            },
        );
    }

    Ok(definitions)
}

#[test]
fn test() {
    let defs = r#"
    // comment
    5000;table_1;col_1,col_2;i32,i32
    5001;table_2;col_1,col_2;i32,i32
    "#;

    let mut parsed_defs = HashMap::new();
    let fields: Vec<String> = vec!["col_1", "col_2"]
        .into_iter()
        .map(String::from)
        .collect();
    let types: Vec<String> = vec!["i32", "i32"].into_iter().map(String::from).collect();
    parsed_defs.insert(
        5000,
        TableDefinition {
            name: "table_1".to_owned(),
            fields: fields.clone(),
            types: types.clone(),
        },
    );
    parsed_defs.insert(
        5001,
        TableDefinition {
            name: "table_2".to_owned(),
            fields,
            types,
        },
    );

    assert_eq!(parse(defs).unwrap(), parsed_defs);
}
