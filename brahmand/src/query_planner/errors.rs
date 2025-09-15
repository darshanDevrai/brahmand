use thiserror::Error;

use crate::query_planner::{
    logical_plan::errors::LogicalPlanError, optimizer::errors::OptimizerError,
};

use super::analyzer::errors::AnalyzerError;

#[derive(Debug, Error)]
pub enum QueryPlannerError {
    #[error("LogicalPlanError: {0}")]
    LogicalPlan(#[from] LogicalPlanError),
    #[error("OptimizerError: {0}")]
    Optimizer(#[from] OptimizerError),
    #[error("AnalyzerError: {0}")]
    Analyzer(#[from] AnalyzerError),
    // #[error("RenderBuildError: {0}")]
    // RenderBuild(#[from] RenderBuildError),
    #[error("Logical Plan Extractor")]
    LogicalPlanExtractor,
}
