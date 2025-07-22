use thiserror::Error;




#[derive(Debug, Clone, Error, PartialEq)]
pub enum PlanCtxError {
    #[error("No table context for alias.")]
    MissingTableCtx,

    #[error("No table context for node alias.")]
    MissingNodeTableCtx,

    #[error("No table context for relationship alias.")]
    MissingRelTableCtx,
}