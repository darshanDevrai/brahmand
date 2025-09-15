use clickhouse::Row;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
pub struct QueryRequest {
    pub query: String,
    pub format: Option<OutputFormat>,
}

// #[derive(Debug, Serialize)]
// #[serde(untagged)]
// pub enum ResponseRows {
//     Value(Vec<Value>),
//     Other(Vec<String>)
// }

// #[derive(Debug, Serialize)]
// pub struct QueryResponse(pub ResponseRows);

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub enum OutputFormat {
    JSONEachRow,
    Pretty,
    PrettyCompact,
    Csv,
    CSVWithNames,
}

impl From<OutputFormat> for String {
    fn from(value: OutputFormat) -> Self {
        match value {
            OutputFormat::JSONEachRow => "JSONEachRow".to_string(),
            OutputFormat::Pretty => "Pretty".to_string(),
            OutputFormat::PrettyCompact => "PrettyCompact".to_string(),
            OutputFormat::Csv => "CSV".to_string(),
            OutputFormat::CSVWithNames => "CSVWithNames".to_string(),
        }
    }
}

#[derive(Debug, Row, Serialize, Deserialize)]
pub struct GraphCatalog {
    pub id: u64,
    pub schema_json: String,
}

// #[derive(Debug, Serialize, Deserialize, Clone)]
// pub struct NodeSchema {
//     pub table_name: String,
//     pub column_names: Option<String>,
// }

// #[derive(Debug, Serialize, Deserialize, Clone)]
// pub struct RelationshipSchema {
//     pub table_name: String,
//     pub column_names: Option<String>,
//     pub from_node: String,
//     pub to_node: String
// }
