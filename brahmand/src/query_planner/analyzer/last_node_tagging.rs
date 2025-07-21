use std::sync::Arc;

use crate::query_planner::{analyzer::analyzer_pass::AnalyzerPass, logical_plan::{logical_plan::LogicalPlan, plan_ctx::PlanCtx}, transformed::Transformed};








pub struct LastNodeTagging;



impl AnalyzerPass for LastNodeTagging {
    fn analyze(&self, logical_plan: Arc<LogicalPlan>, plan_ctx: &mut PlanCtx) -> Transformed<Arc<LogicalPlan>> {


        match logical_plan.as_ref() {
            LogicalPlan::GraphNode(graph_node) => {
                // tag the first node as the last node in the graph traversal
                if plan_ctx.last_node.is_empty() {
                    plan_ctx.last_node = graph_node.alias.clone();
                }
                Transformed::No(logical_plan.clone())
            },
            LogicalPlan::GraphRel(graph_rel) => {
                // process left node first. 
                self.analyze(graph_rel.left.clone(), plan_ctx);

                // If last node is still not found then check at the right tree
                if plan_ctx.last_node.is_empty() {
                    self.analyze(graph_rel.right.clone(), plan_ctx);
                }
                Transformed::No(logical_plan.clone())
            },
            LogicalPlan::Projection(projection) => {
            
                let child_tf = self.analyze(projection.input.clone(), plan_ctx);
                projection.rebuild_or_clone(child_tf, logical_plan.clone())
            },
            LogicalPlan::GroupBy(group_by   ) => {
                let child_tf = self.analyze(group_by.input.clone(), plan_ctx);
                group_by.rebuild_or_clone(child_tf, logical_plan.clone())
            },
            LogicalPlan::Cte(cte   ) => {
                let child_tf = self.analyze( cte.input.clone(), plan_ctx);
                cte.rebuild_or_clone(child_tf, logical_plan.clone())
            },
            LogicalPlan::Scan(_) => {
                Transformed::No(logical_plan.clone())
            },
            LogicalPlan::Empty => Transformed::No(logical_plan.clone()),
            LogicalPlan::GraphJoins(graph_joins) => {
                let child_tf = self.analyze(graph_joins.input.clone(), plan_ctx);
                graph_joins.rebuild_or_clone(child_tf, logical_plan.clone())
            },
            LogicalPlan::Filter(filter) => {
                let child_tf = self.analyze(filter.input.clone(), plan_ctx);
                filter.rebuild_or_clone(child_tf, logical_plan.clone())
            },
            LogicalPlan::OrderBy(order_by) => {
                let child_tf = self.analyze(order_by.input.clone(), plan_ctx);
                order_by.rebuild_or_clone(child_tf, logical_plan.clone())
            },
            LogicalPlan::Skip(skip) => {
                let child_tf = self.analyze(skip.input.clone(), plan_ctx);
                skip.rebuild_or_clone(child_tf, logical_plan.clone())
            },
            LogicalPlan::Limit(limit) => {
                let child_tf = self.analyze(limit.input.clone(), plan_ctx);
                limit.rebuild_or_clone(child_tf, logical_plan.clone())
            },
        }

    }
}

impl LastNodeTagging {
    pub fn new() -> Self {
        LastNodeTagging
    }
    
}