use std::sync::Arc;

use crate::{query_engine::types::GraphSchema, query_engine_v2::{analyzer::{analyzer_pass::AnalyzerPass, filter_tagging::FilterTagging, group_by_building::GroupByBuilding, projection_tagging::ProjectionTagging, schema_inference::SchemaInference}, logical_plan::logical_plan::{LogicalPlan, PlanCtx}}};






mod analyzer_pass;
mod projection_tagging;
mod filter_tagging;
mod group_by_building;
mod schema_inference;
mod errors;


pub fn initial_analyzing(plan: Arc<LogicalPlan>, plan_ctx: &mut PlanCtx) -> Arc<LogicalPlan> {

    // println!("\n plan_ctx Before {} \n\n", plan_ctx);
    // println!("\n\n PLAN Before  {} \n\n", plan);

    let projection_tagging = ProjectionTagging::new();
    let transformed_plan = projection_tagging.analyze(plan.clone(), plan_ctx);
    let plan = transformed_plan.get_plan();


    let group_by_building = GroupByBuilding::new();
    let transformed_plan = group_by_building.analyze(plan.clone(), plan_ctx);
    let plan = transformed_plan.get_plan();

    // println!("\n\n PLAN After  {:#?} \n\n", plan);


    let filter_tagging = FilterTagging::new();
    let transformed_plan = filter_tagging.analyze(plan.clone(), plan_ctx);
    let plan = transformed_plan.get_plan();
    



    // println!("\n plan_ctx After {} \n\n", plan_ctx);
    // println!("\n PLAN After {} \n\n", plan);

    // println!("\n DEBUG PLAN After:\n{:#?}", plan);

    plan
}


pub fn final_analyzing(plan: Arc<LogicalPlan>, plan_ctx: &mut PlanCtx, current_graph_schema: &GraphSchema) -> Arc<LogicalPlan> {
    
    let schema_inference = SchemaInference::new();
    let transformed_plan = schema_inference.analyze_with_graph_schema(plan.clone(), plan_ctx, current_graph_schema);
    let plan = transformed_plan.get_plan();


    plan
}