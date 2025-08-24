


use thiserror::Error;




#[derive(Debug, Clone, Error, PartialEq)]
pub enum RenderBuildError {
    #[error("No From Table.")]
    MissingFromTable,

    #[error("No Select items.")]
    MissingSelectItems,

    #[error("Malformed CTE name.")]
    MalformedCTEName,

    
}

