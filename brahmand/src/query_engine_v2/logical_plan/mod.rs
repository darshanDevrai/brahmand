use std::sync::Arc;

use crate::{open_cypher_parser::ast::OpenCypherQueryAst, query_engine_v2::logical_plan::{errors::PlannerError, logical_plan::{LogicalPlan, PlanCtx}}};

pub mod logical_plan;
pub mod generator;
mod match_clause;
mod where_clause;
mod return_clause;
mod order_by_clause;
mod skip_n_limit_clause;
pub mod errors;
// mod schema_inference;


pub fn evaluate_query(query_ast: OpenCypherQueryAst<'_>) -> Result<(Arc<LogicalPlan>, PlanCtx), PlannerError> {
    generator::generate_logical_plan(&query_ast)
}