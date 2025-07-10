use std::sync::Arc;

use crate::{open_cypher_parser::ast::{LimitClause, SkipClause}, query_engine_v2::logical_plan::logical_plan::{Limit, LogicalPlan, Skip}};







pub fn evaluate_skip_clause(
    skip_clause: &SkipClause,
    plan: LogicalPlan,
) -> LogicalPlan {
    
    LogicalPlan::Skip(Skip{
        input: Arc::new(plan), 
        count: skip_clause.skip_item
    })
}


pub fn evaluate_limit_clause(
    limit_clause: &LimitClause,
    plan: LogicalPlan,
) -> LogicalPlan {
    
    LogicalPlan::Limit(Limit{
        input: Arc::new(plan), 
        count: limit_clause.limit_item
    })
}