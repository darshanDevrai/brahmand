use std::sync::Arc;

use analyzer_pass::AnalyzerResult;

use crate::{
    graph_catalog::graph_schema::GraphSchema,
    query_planner::{
        analyzer::{
            analyzer_pass::AnalyzerPass, duplicate_scans_removing::DuplicateScansRemoving,
            filter_tagging::FilterTagging, graph_join_inference::GraphJoinInference,
            graph_traversal_planning::GraphTRaversalPlanning, group_by_building::GroupByBuilding,
            plan_sanitization::PlanSanitization, projection_tagging::ProjectionTagging,
            query_validation::QueryValidation, schema_inference::SchemaInference,
        },
        logical_plan::logical_plan::LogicalPlan,
    },
};

use super::plan_ctx::PlanCtx;

mod analyzer_pass;
mod duplicate_scans_removing;
pub mod errors;
mod filter_tagging;
mod graph_context;
mod graph_join_inference;
mod graph_traversal_planning;
mod group_by_building;
mod plan_sanitization;
mod projection_tagging;
mod query_validation;
mod schema_inference;

pub fn initial_analyzing(
    plan: Arc<LogicalPlan>,
    plan_ctx: &mut PlanCtx,
    current_graph_schema: &GraphSchema,
) -> AnalyzerResult<Arc<LogicalPlan>> {
    // println!("\n plan_ctx Before {} \n\n", plan_ctx);
    // println!("\n\n PLAN Before  {} \n\n", plan);

    // For initial schema inference, we do not propogate the error. We will try to infer schema in this initial pass. If not able to infer then it will be done in the later pass after projection and filter tagging.
    let schema_inference = SchemaInference::new();
    let plan = if let Ok(transformed_plan) =
        schema_inference.analyze_with_graph_schema(plan.clone(), plan_ctx, current_graph_schema)
    {
        transformed_plan.get_plan()
    } else {
        plan
    };

    let filter_tagging = FilterTagging::new();
    let transformed_plan = filter_tagging.analyze(plan.clone(), plan_ctx)?;
    let plan = transformed_plan.get_plan();

    let projection_tagging = ProjectionTagging::new();
    let transformed_plan = projection_tagging.analyze_with_graph_schema(
        plan.clone(),
        plan_ctx,
        current_graph_schema,
    )?;
    let plan = transformed_plan.get_plan();

    let group_by_building = GroupByBuilding::new();
    let transformed_plan = group_by_building.analyze(plan.clone(), plan_ctx)?;
    let plan = transformed_plan.get_plan();

    // println!("\n\n PLAN After  {:#?} \n\n", plan);

    // println!("\n plan_ctx After initial {} \n\n", plan_ctx);
    // println!("\n PLAN After {} \n\n", plan);

    // println!("\n DEBUG PLAN After:\n{:#?}", plan);

    Ok(plan)
}

pub fn intermediate_analyzing(
    plan: Arc<LogicalPlan>,
    plan_ctx: &mut PlanCtx,
    current_graph_schema: &GraphSchema,
) -> AnalyzerResult<Arc<LogicalPlan>> {
    // println!("\n plan_ctx Before intermediate_analyzing {} \n\n", plan_ctx);
    // println!("\n\n PLAN Before intermediate_analyzing {} \n\n", plan);

    let schema_inference = SchemaInference::new();
    let transformed_plan =
        schema_inference.analyze_with_graph_schema(plan.clone(), plan_ctx, current_graph_schema)?;
    let plan = transformed_plan.get_plan();

    let query_validation = QueryValidation::new();
    let transformed_plan =
        query_validation.analyze_with_graph_schema(plan.clone(), plan_ctx, current_graph_schema)?;
    let plan = transformed_plan.get_plan();

    let graph_traversal_planning = GraphTRaversalPlanning::new();
    let transformed_plan = graph_traversal_planning.analyze_with_graph_schema(
        plan.clone(),
        plan_ctx,
        current_graph_schema,
    )?;
    let plan = transformed_plan.get_plan();

    let transformed_plan = schema_inference.push_inferred_table_names_to_scan(plan, plan_ctx)?;
    let plan = transformed_plan.get_plan();

    let duplicate_scans_removing = DuplicateScansRemoving::new();
    let transformed_plan = duplicate_scans_removing.analyze(plan.clone(), plan_ctx)?;
    let plan = transformed_plan.get_plan();

    let graph_join_inference = GraphJoinInference::new();
    let transformed_plan = graph_join_inference.analyze_with_graph_schema(
        plan.clone(),
        plan_ctx,
        current_graph_schema,
    )?;
    let plan = transformed_plan.get_plan();

    // println!("\n plan_ctx After intermediate_analyzing {} \n\n", plan_ctx);
    // println!("\n\n PLAN After intermediate_analyzing {} \n\n", plan);

    Ok(plan)
}

pub fn final_analyzing(
    plan: Arc<LogicalPlan>,
    plan_ctx: &mut PlanCtx,
    _: &GraphSchema,
) -> AnalyzerResult<Arc<LogicalPlan>> {
    let plan_sanitization = PlanSanitization::new();
    let transformed_plan = plan_sanitization.analyze(plan.clone(), plan_ctx)?;
    let plan = transformed_plan.get_plan();

    Ok(plan)
}
