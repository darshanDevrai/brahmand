use std::sync::Arc;

use crate::query_engine_v2::{logical_plan::logical_plan::{LogicalPlan, PlanCtx}, transformed::Transformed};




pub trait OptimizerPass {
    fn optimize(&self, logical_plan: Arc<LogicalPlan>, plan_ctx: &mut PlanCtx) -> Transformed<Arc<LogicalPlan>>;
}