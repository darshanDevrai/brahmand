use thiserror::Error;


// #[derive(Debug, Error)]
// pub enum QueryEngineError {
//     #[error("ChQueryGeneratorError: {0}")]
//     QueryGenerator(#[from] ChQueryGeneratorError),
//     #[error("OptimizerError: {0}")]
//     Optimizer(#[from] OptimizerError),
//     #[error("PlannerError: {0}")]
//     Planner(#[from] PlannerError),
//     #[error("Unsupported query type found.")]
//     UnsupportedQueryType,
//     // #[error("Should be a DDL query")]
//     // InvalidDDLQueryType,
// }

