use thiserror::Error;

#[derive(Debug, Clone, Error, PartialEq)]
pub enum PlanCtxError {
    #[error("No table context for alias `{alias}`")]
    TableCtx { alias: String },

    #[error("No table context for node alias `{alias}`")]
    NodeTableCtx { alias: String },

    #[error("No table context for relationship alias `{alias}`")]
    RelTableCtx { alias: String },

    #[error("Missing Label for `{alias}`")]
    Label { alias: String },
}
