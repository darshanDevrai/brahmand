use std::sync::Arc;

use crate::query_engine_v2::{logical_plan::logical_plan::{LogicalPlan, PlanCtx}, optimizer::{optimizer_pass::OptimizerPass, projection_push_down::ProjectionPushDown, projection_tagging::ProjectionTagging}};



mod optimizer_pass;
mod projection_tagging;
mod projection_push_down;
mod filter_tagging;


pub fn optimize(plan: Arc<LogicalPlan>, plan_ctx: &mut PlanCtx) -> Arc<LogicalPlan> {
    // let mut projection_push_down = ProjectionPushDown::new();
    // projection_push_down.tag_projections_to_tables(&mut plan, plan_ctx);
    // plan

    let projection_tagging = ProjectionTagging::new();
    let transformed_plan = projection_tagging.optimize(plan.clone(), plan_ctx);
    let plan = transformed_plan.get_plan();

    let projection_push_down = ProjectionPushDown::new();
    let transformed_plan = projection_push_down.optimize(plan.clone(), plan_ctx);
    let plan = transformed_plan.get_plan();

    println!("\n\n PLAN Outer {} \n\n", plan);
    println!("\n\n plan_ctx Outer {:?} \n\n", plan_ctx);

    plan
}