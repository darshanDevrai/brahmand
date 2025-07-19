use std::sync::Arc;

use crate::{open_cypher_parser::ast::{LimitClause, SkipClause}, query_engine_v2::logical_plan::logical_plan::{Limit, LogicalPlan, Skip}};


pub fn evaluate_skip_clause(
    skip_clause: &SkipClause,
    plan: Arc<LogicalPlan>,
) -> Arc<LogicalPlan> {
    
    Arc::new(LogicalPlan::Skip(Skip{
        input: plan, 
        count: skip_clause.skip_item
    }))
}


pub fn evaluate_limit_clause(
    limit_clause: &LimitClause,
    plan: Arc<LogicalPlan>,
) -> Arc<LogicalPlan> {
    
    Arc::new(LogicalPlan::Limit(Limit{
        input: plan, 
        count: limit_clause.limit_item
    }))
}