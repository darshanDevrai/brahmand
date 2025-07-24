use thiserror::Error;



#[derive(Debug, Clone, Error, PartialEq)]
pub enum GraphSchemaError {

    #[error("No relationship schema found for `{rel_label}`.")]
    NoRelationSchemaFound{rel_label: String},
    #[error("No node schema found for `{node_label}`")]
    NoNodeSchemaFound{node_label: String},

}
