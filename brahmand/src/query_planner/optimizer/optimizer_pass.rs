use std::sync::Arc;

use crate::query_planner::{
    logical_plan::logical_plan::LogicalPlan, optimizer::errors::OptimizerError,
    plan_ctx::plan_ctx::PlanCtx, transformed::Transformed,
};

pub type OptimizerResult<T> = Result<T, OptimizerError>;

pub trait OptimizerPass {
    fn optimize(
        &self,
        logical_plan: Arc<LogicalPlan>,
        plan_ctx: &mut PlanCtx,
    ) -> OptimizerResult<Transformed<Arc<LogicalPlan>>>;
}
