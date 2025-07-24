use thiserror::Error;




#[derive(Debug, Clone, Error, PartialEq)]
pub enum OptimizerError {
    #[error("Error while combining filter predicates")]
    CombineFilterPredicate,
    #[error("While rotating the plan, new plan must be a graph rel.")]
    MissingGraphRelInRotatePlan,
}
