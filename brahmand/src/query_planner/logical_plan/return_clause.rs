use crate::{
    open_cypher_parser::ast::ReturnClause,
    query_planner::logical_plan::{LogicalPlan, Projection, ProjectionItem},
};
use std::sync::Arc;

pub fn evaluate_return_clause<'a>(
    return_clause: &ReturnClause<'a>,
    plan: Arc<LogicalPlan>,
) -> Arc<LogicalPlan> {
    let projection_items: Vec<ProjectionItem> = return_clause
        .return_items
        .iter()
        .map(|item| item.clone().into())
        .collect();
    Arc::new(LogicalPlan::Projection(Projection {
        input: plan,
        items: projection_items,
    }))
}
