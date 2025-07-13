use std::sync::Arc;

use crate::query_engine_v2::{analyzer::analyzer_pass::AnalyzerPass, expr::plan_expr::PlanExpr, logical_plan::logical_plan::{GroupBy, LogicalPlan, PlanCtx, ProjectionItem}, transformed::Transformed};




pub struct GroupByBuilding;

// In the final projections, if there is an aggregate fn then add other projections in group by clause
// build group by plan after projection tagging.
impl AnalyzerPass for GroupByBuilding {
    fn analyze(&self, logical_plan: Arc<LogicalPlan>, plan_ctx: &mut PlanCtx) -> Transformed<Arc<LogicalPlan>> {
        match logical_plan.as_ref() {
            LogicalPlan::Projection(projection) => {
                
                let non_agg_projections: Vec<ProjectionItem> = projection.items.iter().filter(|item| !matches!(item.expression, PlanExpr::AggregateFnCall(_))).cloned().collect();
                

                if non_agg_projections.len() < projection.items.len() {
                    // aggregate fns found. Build the groupby plan here
                    return Transformed::Yes(Arc::new(LogicalPlan::GroupBy(GroupBy{
                        input: logical_plan.clone(),
                        expressions: non_agg_projections.into_iter().map(|item| item.expression).collect(),
                    })))
                }

                let child_tf = self.analyze(projection.input.clone(), plan_ctx);
                projection.rebuild_or_clone(child_tf, logical_plan.clone())
            },
            LogicalPlan::GroupBy(group_by   ) => {
                let child_tf = self.analyze(group_by.input.clone(), plan_ctx);
                group_by.rebuild_or_clone(child_tf, logical_plan.clone())
            },
            LogicalPlan::GraphNode(graph_node) => {
                let child_tf = self.analyze(graph_node.input.clone(), plan_ctx);
                let self_tf = self.analyze(graph_node.self_plan.clone(), plan_ctx);
                graph_node.rebuild_or_clone(child_tf, self_tf, logical_plan.clone())
            },
            LogicalPlan::GraphRel(graph_rel) => {
                let left_tf = self.analyze(graph_rel.left.clone(), plan_ctx);
                let center_tf = self.analyze(graph_rel.center.clone(), plan_ctx);
                let right_tf = self.analyze(graph_rel.right.clone(), plan_ctx);
                graph_rel.rebuild_or_clone(left_tf, center_tf, right_tf, logical_plan.clone())
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

impl GroupByBuilding {
    pub fn new() -> Self {
        GroupByBuilding
    }
}

