use std::sync::Arc;

use crate::{
    open_cypher_parser::ast::OrderByClause,
    query_planner::logical_plan::{LogicalPlan, OrderBy, OrderByItem},
};

pub fn evaluate_order_by_clause<'a>(
    order_by_clause: &OrderByClause<'a>,
    plan: Arc<LogicalPlan>,
) -> Arc<LogicalPlan> {
    let predicates: Vec<OrderByItem> = order_by_clause
        .order_by_items
        .iter()
        .map(|item| item.clone().into())
        .collect();
    Arc::new(LogicalPlan::OrderBy(OrderBy {
        input: plan,
        items: predicates,
    }))
}
