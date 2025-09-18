use std::sync::Arc;

use crate::query_planner::{
    logical_plan::LogicalPlan,
    optimizer::{
        anchor_node_selection::AnchorNodeSelection,
        filter_push_down::FilterPushDown,
        optimizer_pass::{OptimizerPass, OptimizerResult},
        projection_push_down::ProjectionPushDown,
    },
};

use super::plan_ctx::PlanCtx;

mod anchor_node_selection;
pub mod errors;
mod filter_push_down;
mod optimizer_pass;
mod projection_push_down;

pub fn initial_optimization(
    plan: Arc<LogicalPlan>,
    plan_ctx: &mut PlanCtx,
) -> OptimizerResult<Arc<LogicalPlan>> {
    let anchor_node_selection = AnchorNodeSelection::new();
    let transformed_plan = anchor_node_selection.optimize(plan.clone(), plan_ctx)?;
    let plan = transformed_plan.get_plan();

    Ok(plan)
}

pub fn final_optimization(
    plan: Arc<LogicalPlan>,
    plan_ctx: &mut PlanCtx,
) -> OptimizerResult<Arc<LogicalPlan>> {
    let projection_push_down = ProjectionPushDown::new();
    let transformed_plan = projection_push_down.optimize(plan.clone(), plan_ctx)?;
    let plan = transformed_plan.get_plan();

    let filter_push_down = FilterPushDown::new();
    let transformed_plan = filter_push_down.optimize(plan.clone(), plan_ctx)?;
    let plan = transformed_plan.get_plan();

    // println!("\n plan_ctx After {} \n\n", plan_ctx);
    // println!("\n PLAN After {} \n\n", plan);

    // println!("\n DEBUG PLAN After:\n{:#?}", plan);

    Ok(plan)
}
