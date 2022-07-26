use crate::Error;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableDefinition {
    pub name: String,
    pub columns: Vec<String>,
    pub types: Vec<String>,
}

pub type TableDefinitions = HashMap<u16, TableDefinition>;

pub fn parse(contents: &str) -> Result<TableDefinitions, Error> {
    let mut definitions = HashMap::new();

    for line in contents.lines() {
        let line = line.trim();
        if line.starts_with("//") || line.is_empty() {
            continue;
        }

        let mut line = line.split(';');
        let id = line
            .next()
            .unwrap() // PANIC split on string always returns at least one item
            .parse()
            .map_err(Error::InvalidTableId)?;
        let name = line.next().ok_or(Error::NoTableName)?.to_owned();
        let columns: Vec<String> = line
            .next()
            .ok_or(Error::NoTableColumnNames)?
            .split(',')
            .map(String::from)
            .collect();
        let types: Vec<String> = line
            .next()
            .ok_or(Error::NoTableColumnTypes)?
            .split(',')
            .map(String::from)
            .collect();

        if columns.len() != types.len() {
            return Err(Error::InconsistentNamesAndTypesLength);
        }

        definitions.insert(
            id,
            TableDefinition {
                name,
                columns,
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
    let columns: Vec<String> = vec!["col_1", "col_2"]
        .into_iter()
        .map(String::from)
        .collect();
    let types: Vec<String> = vec!["i32", "i32"].into_iter().map(String::from).collect();
    parsed_defs.insert(
        5000,
        TableDefinition {
            name: "table_1".to_owned(),
            columns: columns.clone(),
            types: types.clone(),
        },
    );
    parsed_defs.insert(
        5001,
        TableDefinition {
            name: "table_2".to_owned(),
            columns,
            types,
        },
    );

    assert_eq!(parse(defs).unwrap(), parsed_defs);
}
