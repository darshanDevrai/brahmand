use thiserror::Error;

#[derive(Debug, Clone, Error, PartialEq)]
pub enum PlanCtxError {
    #[error("No table context for alias `{alias}`")]
    MissingTableCtx { alias: String },

    #[error("No table context for node alias `{alias}`")]
    MissingNodeTableCtx { alias: String },

    #[error("No table context for relationship alias `{alias}`")]
    MissingRelTableCtx { alias: String },

    #[error("Missing Label for `{alias}`")]
    MissingLabel { alias: String },
}
