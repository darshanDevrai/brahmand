use thiserror::Error;

use crate::query_planner::plan_ctx::errors::PlanCtxError;




#[derive(Debug, Clone, Error, PartialEq)]
pub enum AnalyzerError {

    #[error(
        "No relation label found. Currently we need label to identify the relationship table. This will change in future."
    )]
    MissingRelationLabel,
    // #[error("No traversal sequence found.")]
    // NoTravelsalSequence,
    #[error("No traversal graph found.")]
    NoTravelsalGraph,
    #[error("No relationship schema found.")]
    NoRelationSchemaFound,
    #[error("No node schema found.")]
    NoNodeSchemaFound,
    #[error("Not enough information. Labels are required to identify nodes and relationships")]
    NotEnoughLabels,

    #[error("PlanCtxError: {0}")]
    PlanCtx(#[from] PlanCtxError),

    // #[error("Non CTE plan found. Expected CTE.")]
    // NonCTEPlanFound,
}
