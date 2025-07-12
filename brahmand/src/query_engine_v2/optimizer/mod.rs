use std::sync::Arc;

use crate::query_engine_v2::{logical_plan::logical_plan::{LogicalPlan, PlanCtx}, optimizer::{anchor_node_selection::AnchorNodeSelection, filter_push_down::FilterPushDown, filter_tagging::FilterTagging, optimizer_pass::OptimizerPass, projection_push_down::ProjectionPushDown, projection_tagging::ProjectionTagging}};



mod optimizer_pass;
mod projection_tagging;
mod projection_push_down;
mod filter_tagging;
mod filter_push_down;
mod anchor_node_selection;


pub fn optimize(plan: Arc<LogicalPlan>, plan_ctx: &mut PlanCtx) -> Arc<LogicalPlan> {
    // let mut projection_push_down = ProjectionPushDown::new();
    // projection_push_down.tag_projections_to_tables(&mut plan, plan_ctx);
    // plan

    // println!("\n plan_ctx Before {} \n\n", plan_ctx);
    println!("\n\n PLAN Before  {} \n\n", plan);

    let projection_tagging = ProjectionTagging::new();
    let transformed_plan = projection_tagging.optimize(plan.clone(), plan_ctx);
    let plan = transformed_plan.get_plan();

    let filter_tagging = FilterTagging::new();
    let transformed_plan = filter_tagging.optimize(plan.clone(), plan_ctx);
    let plan = transformed_plan.get_plan();

    let anchor_node_selection = AnchorNodeSelection::new();
    let transformed_plan = anchor_node_selection.optimize(plan.clone(), plan_ctx);
    let plan = transformed_plan.get_plan();

    let projection_push_down = ProjectionPushDown::new();
    let transformed_plan = projection_push_down.optimize(plan.clone(), plan_ctx);
    let plan = transformed_plan.get_plan();


    let filter_push_down = FilterPushDown::new();
    let transformed_plan = filter_push_down.optimize(plan.clone(), plan_ctx);
    let plan = transformed_plan.get_plan();

    



    // println!("\n plan_ctx After {} \n\n", plan_ctx);
    println!("\n PLAN After {} \n\n", plan);

    // println!("\n DEBUG PLAN After:\n{:#?}", plan);

    plan
}

