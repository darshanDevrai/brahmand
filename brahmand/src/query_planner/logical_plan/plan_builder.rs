use std::sync::Arc;

use crate::{
    open_cypher_parser::ast::OpenCypherQueryAst,
    query_planner::{
        logical_plan::{
            errors::LogicalPlanError, logical_plan::LogicalPlan, match_clause, order_by_clause,
            return_clause, skip_n_limit_clause, where_clause,
        },
        plan_ctx::PlanCtx,
    },
};

pub type LogicalPlanResult<T> = Result<T, LogicalPlanError>;

pub fn build_logical_plan(
    query_ast: &OpenCypherQueryAst,
) -> LogicalPlanResult<(Arc<LogicalPlan>, PlanCtx)> {
    let mut logical_plan: Arc<LogicalPlan> = Arc::new(LogicalPlan::Empty);
    let mut plan_ctx = PlanCtx::default();

    if let Some(match_clause) = &query_ast.match_clause {
        logical_plan =
            match_clause::evaluate_match_clause(match_clause, logical_plan, &mut plan_ctx)?;
    }

    if let Some(where_clause) = &query_ast.where_clause {
        logical_plan = where_clause::evaluate_where_clause(where_clause, logical_plan);
    }

    if let Some(return_clause) = &query_ast.return_clause {
        logical_plan = return_clause::evaluate_return_clause(return_clause, logical_plan);
    }

    if let Some(order_clause) = &query_ast.order_by_clause {
        logical_plan = order_by_clause::evaluate_order_by_clause(order_clause, logical_plan);
    }

    if let Some(skip_clause) = &query_ast.skip_clause {
        logical_plan = skip_n_limit_clause::evaluate_skip_clause(skip_clause, logical_plan);
    }

    if let Some(limit_clause) = &query_ast.limit_clause {
        logical_plan = skip_n_limit_clause::evaluate_limit_clause(limit_clause, logical_plan);
    }

    Ok((logical_plan, plan_ctx))
}
