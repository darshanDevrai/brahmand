use std::sync::Arc;

use crate::{open_cypher_parser::ast::OpenCypherQueryAst, query_planner::logical_plan::{logical_plan::LogicalPlan, plan_builder::LogicalPlanResult}};

use super::plan_ctx::plan_ctx::PlanCtx;

pub mod logical_plan;
pub mod plan_builder;
mod match_clause;
mod where_clause;
mod return_clause;
mod order_by_clause;
mod skip_n_limit_clause;
pub mod errors;


pub fn evaluate_query(query_ast: OpenCypherQueryAst<'_>) -> LogicalPlanResult<(Arc<LogicalPlan>, PlanCtx)> {
    plan_builder::build_logical_plan(&query_ast)
}