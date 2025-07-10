use thiserror::Error;

#[derive(Debug, Clone, Error, PartialEq)]
pub enum PlannerError {
    #[error(
        "Empty node or relationship found. Currently it is not supported. This will change in future."
    )]
    EmptyNode,
    #[error("Unexpected error found. Entity Uid is present but table data is missing.")]
    Unexpected,
    #[error(
        "Orphan property found in return clause. A property must be of particular node/relationship."
    )]
    OrphanPropertyInReturnClause,
    #[error(
        "Orphan property access found in return clause. A property access must be of particular node/relationship."
    )]
    OrphanPropertyAccessInReturnClause,
    #[error("Unsupported expression found in return clause.")]
    UnsupportedItemInReturnClause,
    #[error("Invalid variable in order by clause.")]
    InvalidVariableInOrderByClause,
    #[error(
        "Orphan property access found in order by clause. A property access must be of particular node/relationship."
    )]
    OrphanPropertyAccessInOrderByClause,
    #[error("Unsupported expression found in order by clause.")]
    UnsupportedtemInOrderByClause,
    #[error("Invalid variable found in return clause.")]
    InvalidVariableInReturnClause,
    #[error("Invalid property access found in return clause.")]
    InvalidPropAccessInReturnClause,
    #[error("Invalid property access found in order by clause.")]
    InvalidPropAccessInOrderByClause,
}
