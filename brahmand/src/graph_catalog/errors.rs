use thiserror::Error;

#[derive(Debug, Clone, Error, PartialEq)]
pub enum GraphSchemaError {
    #[error("No relationship schema found for `{rel_label}`.")]
    Relation { rel_label: String },
    #[error("No node schema found for `{node_label}`")]
    Node { node_label: String },
    #[error("No relationship index schema found for `{rel_label}`.")]
    RelationIndex { rel_label: String },
}
