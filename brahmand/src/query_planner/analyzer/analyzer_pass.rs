use std::sync::Arc;

use crate::{graph_schema::graph_schema::GraphSchema, query_planner::{logical_plan::logical_plan::LogicalPlan, plan_ctx::plan_ctx::PlanCtx, transformed::Transformed}};

use super::errors::AnalyzerError;





pub type AnalyzerResult<T> = Result<T, AnalyzerError>;


pub trait AnalyzerPass {
    fn analyze(&self, logical_plan: Arc<LogicalPlan>, plan_ctx: &mut PlanCtx) -> AnalyzerResult<Transformed<Arc<LogicalPlan>>> {
        Ok(Transformed::No(logical_plan.clone()))
    }

    fn analyze_with_graph_schema(&self, logical_plan: Arc<LogicalPlan>, plan_ctx: &mut PlanCtx, graph_schema: &GraphSchema) -> AnalyzerResult<Transformed<Arc<LogicalPlan>>> {
        Ok(Transformed::No(logical_plan.clone()))
    }
}