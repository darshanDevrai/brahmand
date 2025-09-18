use thiserror::Error;

#[derive(Debug, Clone, Error, PartialEq)]
pub enum GraphSchemaError {
    #[error("No relationship schema found for `{rel_label}`.")]
    RelationSchemaFound { rel_label: String },
    #[error("No node schema found for `{node_label}`")]
    NodeSchemaFound { node_label: String },
    #[error("No relationship index schema found for `{rel_label}`.")]
    RelationIndexSchemaFound { rel_label: String },
}
