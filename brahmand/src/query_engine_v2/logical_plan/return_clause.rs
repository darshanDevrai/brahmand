use crate::{open_cypher_parser::ast::ReturnClause, query_engine_v2::logical_plan::logical_plan::{LogicalPlan, Projection, ReturnItem}};
use std::sync::Arc;

pub fn evaluate_return_clause<'a>(
    return_clause: &ReturnClause<'a>,
    plan: LogicalPlan,
) -> LogicalPlan {
    
    let return_items: Vec<ReturnItem> = return_clause.return_items.iter().map(|item| item.clone().into()).collect();
    LogicalPlan::Projection(Projection {
        input: Arc::new(plan),
        items: return_items,
    })
}