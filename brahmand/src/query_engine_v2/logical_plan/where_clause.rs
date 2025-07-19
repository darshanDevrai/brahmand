use std::sync::Arc;

use crate::{open_cypher_parser::ast::WhereClause, query_engine_v2::{expr::plan_expr::PlanExpr, logical_plan::logical_plan::{Filter, LogicalPlan}}};


pub fn evaluate_where_clause<'a>(
    where_clause: &WhereClause<'a>,
    plan: Arc<LogicalPlan>,
) -> Arc<LogicalPlan> {
    
    let predicates:PlanExpr = where_clause.conditions.clone().into();
    Arc::new(LogicalPlan::Filter(Filter{
        input: plan, 
        predicate: predicates
    }))
}
