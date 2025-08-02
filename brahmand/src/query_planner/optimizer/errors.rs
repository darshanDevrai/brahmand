use std::fmt::Display;

use thiserror::Error;

use crate::query_planner::plan_ctx::errors::PlanCtxError;


#[derive(Debug, Clone, Error, PartialEq)]
pub enum Pass {
    AnchorNodeSelection,
    ProjectionPushDown,
    FilterPushDown,
}

impl Display for Pass {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Pass::AnchorNodeSelection => write!(f, "AnchorNodeSelection"),
            Pass::ProjectionPushDown => write!(f, "ProjectionPushDown"),
            Pass::FilterPushDown => write!(f, "FilterPushDown"),
        }
    }
}



#[derive(Debug, Clone, Error, PartialEq)]
pub enum OptimizerError {
    #[error("Error while combining filter predicates")]
    CombineFilterPredicate,
    #[error("While rotating the plan, new plan must be a graph rel.")]
    MissingGraphRelInRotatePlan,
    #[error("PlanCtxError: {pass}: {source}.")]
    PlanCtx {
        pass: Pass,
        #[source]
        source: PlanCtxError,
    },
}
