use thiserror::Error;

#[derive(Debug, Clone, Error, PartialEq)]
pub enum LogicalPlanError {
    #[error(
        "Empty node or relationship found. Currently it is not supported. This will change in future."
    )]
    EmptyNode,
    #[error("Parameters are not yet supported in properties.")]
    FoundParamInProperties,
    #[error("Disconnected pattern found.")]
    DisconnectedPatternFound,
}
