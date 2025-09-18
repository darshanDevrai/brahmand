use std::sync::Arc;

use crate::{
    open_cypher_parser::ast::WhereClause,
    query_planner::{
        logical_expr::LogicalExpr,
        logical_plan::{Filter, LogicalPlan},
    },
};

pub fn evaluate_where_clause<'a>(
    where_clause: &WhereClause<'a>,
    plan: Arc<LogicalPlan>,
) -> Arc<LogicalPlan> {
    let predicates: LogicalExpr = where_clause.conditions.clone().into();
    Arc::new(LogicalPlan::Filter(Filter {
        input: plan,
        predicate: predicates,
    }))
}
