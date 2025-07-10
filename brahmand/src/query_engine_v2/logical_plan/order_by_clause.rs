use std::sync::Arc;

use crate::{open_cypher_parser::ast::OrderByClause, query_engine_v2::{expr::plan_expr::PlanExpr, logical_plan::logical_plan::{LogicalPlan, OrderBy, OrderByItem}}};









pub fn evaluate_order_by_clause<'a>(
    order_by_clause: &OrderByClause<'a>,
    plan: LogicalPlan,
) -> LogicalPlan {
    
    let predicates:Vec<OrderByItem> = order_by_clause.order_by_items.iter().map(|item| item.clone().into()).collect();
    LogicalPlan::OrderBy(OrderBy{
        input: Arc::new(plan), 
        items: predicates
    })
}





