use std::sync::Arc;

use crate::query_engine_v2::{analyzer::analyzer_pass::AnalyzerPass, logical_plan::{logical_plan::LogicalPlan, plan_ctx::PlanCtx}, transformed::Transformed};








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
            LogicalPlan::ConnectedTraversal(connected_traversal) => {
                let start_tf = self.analyze(connected_traversal.start_node.clone(), plan_ctx);
                let rel_tf = self.analyze(connected_traversal.relationship.clone(), plan_ctx);
                let end_tf = self.analyze(connected_traversal.end_node.clone(), plan_ctx);
                connected_traversal.rebuild_or_clone(start_tf, rel_tf, end_tf, logical_plan.clone())
            },
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


        // let mut last_node_alias:Option<String> = None;   
        // let transformed_plan = self.get_last_node(logical_plan, &mut last_node_alias);

        // println!("last_node_plan -> {:?}", last_node_alias);

        // transformed_plan

    }
}

impl LastNodeTagging {
    pub fn new() -> Self {
        LastNodeTagging
    }

    fn get_last_node(&self, logical_plan: Arc<LogicalPlan>, last_node_alias: &mut Option<String>) -> Transformed<Arc<LogicalPlan>> {
        match logical_plan.as_ref() {
            LogicalPlan::Projection(projection) => {

                let child_tf = self.get_last_node(projection.input.clone(), last_node_alias);
                projection.rebuild_or_clone(child_tf, logical_plan.clone())
            },
            LogicalPlan::GraphNode(graph_node) => {
                // If only single node is there then graph_node is the last one
                if last_node_alias.is_none() {
                    *last_node_alias = Some(graph_node.alias.clone());
                    // the parent CTE will be removed at Cte's rebuild_or_clone method
                    return Transformed::Yes(Arc::new(LogicalPlan::Empty));
                }
                Transformed::No(logical_plan.clone())

            },
            LogicalPlan::GraphRel(graph_rel) => {
                // first Graph rel's left part is the last node
                let left_tf = self.get_last_node(graph_rel.left.clone(), last_node_alias);
                let center_tf = self.get_last_node(graph_rel.center.clone(), last_node_alias); 
                let right_tf = self.get_last_node(graph_rel.right.clone(), last_node_alias);  
                graph_rel.rebuild_or_clone(left_tf, center_tf, right_tf, logical_plan.clone())
            },
            LogicalPlan::Cte(cte   ) => {
                let child_tf = self.get_last_node( cte.input.clone(), last_node_alias);
                cte.rebuild_or_clone(child_tf, logical_plan.clone())
            },
            LogicalPlan::Scan(_) => {
                Transformed::No(logical_plan.clone())
            },
            LogicalPlan::Empty => Transformed::No(logical_plan.clone()),
            LogicalPlan::ConnectedTraversal(connected_traversal) => {
                let left_tf = self.get_last_node(connected_traversal.start_node.clone(), last_node_alias);
                let rel_tf = self.get_last_node(connected_traversal.relationship.clone(), last_node_alias);
                let right_tf = self.get_last_node(connected_traversal.end_node.clone(), last_node_alias);
                connected_traversal.rebuild_or_clone(left_tf, rel_tf, right_tf, logical_plan.clone())
            },
            LogicalPlan::GraphJoins(graph_joins) => {
                let child_tf = self.get_last_node(graph_joins.input.clone(), last_node_alias);
                graph_joins.rebuild_or_clone(child_tf, logical_plan.clone())
            },
            LogicalPlan::Filter(filter) => {
                let child_tf = self.get_last_node(filter.input.clone(), last_node_alias);
                filter.rebuild_or_clone(child_tf, logical_plan.clone())
            },
            LogicalPlan::GroupBy(group_by   ) => {
                let child_tf = self.get_last_node(group_by.input.clone(), last_node_alias);
                group_by.rebuild_or_clone(child_tf, logical_plan.clone())
            },
            LogicalPlan::OrderBy(order_by) => {
                let child_tf = self.get_last_node(order_by.input.clone(), last_node_alias);
                order_by.rebuild_or_clone(child_tf, logical_plan.clone())
            },
            LogicalPlan::Skip(skip) => {
                let child_tf = self.get_last_node(skip.input.clone(), last_node_alias);
                skip.rebuild_or_clone(child_tf, logical_plan.clone())
            },
            LogicalPlan::Limit(limit) => {
                let child_tf = self.get_last_node(limit.input.clone(), last_node_alias);
                limit.rebuild_or_clone(child_tf, logical_plan.clone())
            },
        }
    }
    
}