use std::sync::Arc;

use crate::{open_cypher_parser::ast::OpenCypherQueryAst, query_engine_v2::logical_plan::{errors::PlannerError, logical_plan::LogicalPlan, plan_ctx::PlanCtx}};

pub mod logical_plan;
pub mod plan_ctx;
pub mod plan_builder;
mod match_clause;
mod where_clause;
mod return_clause;
mod order_by_clause;
mod skip_n_limit_clause;
pub mod errors;


pub fn evaluate_query(query_ast: OpenCypherQueryAst<'_>) -> Result<(Arc<LogicalPlan>, PlanCtx), PlannerError> {
    plan_builder::build_logical_plan(&query_ast)
}