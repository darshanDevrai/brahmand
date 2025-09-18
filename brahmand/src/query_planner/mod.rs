use std::sync::Arc;

use errors::QueryPlannerError;
use types::QueryType;

use crate::{
    graph_catalog::graph_schema::GraphSchema,
    open_cypher_parser::ast::OpenCypherQueryAst,
    query_planner::{analyzer::errors::AnalyzerError, logical_plan::LogicalPlan},
};

pub mod analyzer;
mod errors;
pub mod logical_expr;
pub mod logical_plan;
pub mod optimizer;
pub mod plan_ctx;
pub mod transformed;
pub mod types;

pub fn get_query_type(query_ast: &OpenCypherQueryAst) -> QueryType {
    if query_ast.create_node_table_clause.is_some() || query_ast.create_rel_table_clause.is_some() {
        QueryType::Ddl
    } else if query_ast.delete_clause.is_some() {
        QueryType::Delete
    } else if query_ast.set_clause.is_some() || query_ast.remove_clause.is_some() {
        QueryType::Update
    } else {
        QueryType::Read
    }
}

pub fn evaluate_read_query(
    query_ast: OpenCypherQueryAst,
    current_graph_schema: &GraphSchema,
) -> Result<LogicalPlan, QueryPlannerError> {
    let (logical_plan, mut plan_ctx) = logical_plan::evaluate_query(query_ast)?;

    // println!("\n\n PLAN Before  {} \n\n", logical_plan);
    let logical_plan =
        analyzer::initial_analyzing(logical_plan, &mut plan_ctx, current_graph_schema)?;

    let logical_plan = optimizer::initial_optimization(logical_plan, &mut plan_ctx)?;

    let intermediate_analyzer_result =
        analyzer::intermediate_analyzing(logical_plan, &mut plan_ctx, current_graph_schema);
    // in case of intermediate analyzer, we can get error from query validation pass when there is an issue with relation direction or relation not present.
    // in that case, return the empty match plan and exit from subsequent passes.
    // In case of OPTIONAL MATCH, we have to handle it differently.
    let logical_plan = match intermediate_analyzer_result {
        Ok(plan) => Ok(plan),
        Err(e) => match e {
            AnalyzerError::InvalidRelationInQuery { rel } => {
                println!("Invalid relation in query found {rel}");
                let new_plan = LogicalPlan::get_empty_match_plan();
                return Ok(new_plan);
            }
            _ => Err(e),
        },
    }?;

    // let logical_plan = analyzer::intermediate_analyzing(logical_plan, &mut plan_ctx, current_graph_schema)?;

    let logical_plan = optimizer::final_optimization(logical_plan, &mut plan_ctx)?;

    let logical_plan =
        analyzer::final_analyzing(logical_plan, &mut plan_ctx, current_graph_schema)?;

    // println!("\n\n plan_ctx after \n {}",plan_ctx);
    // println!("\n plan after{}", logical_plan);

    let logical_plan =
        Arc::into_inner(logical_plan).ok_or(QueryPlannerError::LogicalPlanExtractor)?;
    Ok(logical_plan)
}
