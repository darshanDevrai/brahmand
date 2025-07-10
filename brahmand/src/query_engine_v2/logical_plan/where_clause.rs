use std::sync::Arc;

use crate::{open_cypher_parser::ast::WhereClause, query_engine_v2::{expr::plan_expr::PlanExpr, logical_plan::{errors::PlannerError, logical_plan::{Filter, LogicalPlan}, where_clause}}};







pub fn evaluate_where_clause<'a>(
    where_clause: &WhereClause<'a>,
    plan: LogicalPlan,
) -> LogicalPlan {
    
    let predicates:PlanExpr = where_clause.conditions.clone().into();
    LogicalPlan::Filter(Filter{
        input: Arc::new(plan), 
        predicate: predicates
    })
}
