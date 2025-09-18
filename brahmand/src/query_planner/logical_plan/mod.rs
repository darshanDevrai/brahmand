use std::sync::Arc;

use uuid::Uuid;

use crate::{
    open_cypher_parser::ast::OpenCypherQueryAst,
    query_planner::logical_plan::{logical_plan::LogicalPlan, plan_builder::LogicalPlanResult},
};

use super::plan_ctx::PlanCtx;

pub mod errors;
pub mod logical_plan;
mod match_clause;
mod order_by_clause;
pub mod plan_builder;
mod return_clause;
mod skip_n_limit_clause;
mod where_clause;

pub fn evaluate_query(
    query_ast: OpenCypherQueryAst<'_>,
) -> LogicalPlanResult<(Arc<LogicalPlan>, PlanCtx)> {
    plan_builder::build_logical_plan(&query_ast)
}

pub fn generate_id() -> String {
    format!(
        "a{}",
        Uuid::new_v4().to_string()[..10]
            .to_string()
            .replace("-", "")
    )
}
