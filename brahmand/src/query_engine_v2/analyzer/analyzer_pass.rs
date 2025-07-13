use std::sync::Arc;

use crate::{query_engine::types::GraphSchema, query_engine_v2::{logical_plan::logical_plan::{LogicalPlan, PlanCtx}, transformed::Transformed}};








pub trait AnalyzerPass {
    fn analyze(&self, logical_plan: Arc<LogicalPlan>, plan_ctx: &mut PlanCtx) -> Transformed<Arc<LogicalPlan>> {
        Transformed::No(logical_plan.clone())
    }

    fn analyze_with_graph_schema(&self, logical_plan: Arc<LogicalPlan>, plan_ctx: &mut PlanCtx, graph_schema: &GraphSchema) -> Transformed<Arc<LogicalPlan>> {
        Transformed::No(logical_plan.clone())
    }
}