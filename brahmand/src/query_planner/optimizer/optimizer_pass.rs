use std::sync::Arc;

use crate::query_planner::{logical_plan::{logical_plan::LogicalPlan, plan_ctx::PlanCtx}, transformed::Transformed};




pub trait OptimizerPass {
    fn optimize(&self, logical_plan: Arc<LogicalPlan>, plan_ctx: &mut PlanCtx) -> Transformed<Arc<LogicalPlan>>;
}