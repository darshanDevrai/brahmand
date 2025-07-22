use std::sync::Arc;

use crate::query_planner::{analyzer::analyzer_pass::{AnalyzerPass, AnalyzerResult}, logical_expr::logical_expr::LogicalExpr, logical_plan::logical_plan::{GroupBy, LogicalPlan, ProjectionItem}, plan_ctx::plan_ctx::PlanCtx, transformed::Transformed};




pub struct GroupByBuilding;

// In the final projections, if there is an aggregate fn then add other projections in group by clause
// build group by plan after projection tagging.
impl AnalyzerPass for GroupByBuilding {
    fn analyze(&self, logical_plan: Arc<LogicalPlan>, plan_ctx: &mut PlanCtx) -> AnalyzerResult<Transformed<Arc<LogicalPlan>>> {
        match logical_plan.as_ref() {
            LogicalPlan::Projection(projection) => {
                let non_agg_projections: Vec<ProjectionItem> = projection.items.iter().filter(|item| !matches!(item.expression, LogicalExpr::AggregateFnCall(_))).cloned().collect();

                if non_agg_projections.len() < projection.items.len() && !non_agg_projections.is_empty() {
                    // aggregate fns found. Build the groupby plan here
                    return Ok(Transformed::Yes(Arc::new(LogicalPlan::GroupBy(GroupBy{
                        input: logical_plan.clone(),
                        expressions: non_agg_projections.into_iter().map(|item| item.expression).collect(),
                    }))))
                }

                let child_tf = self.analyze(projection.input.clone(), plan_ctx)?;
                Ok(projection.rebuild_or_clone(child_tf, logical_plan.clone()))
            },
            LogicalPlan::GroupBy(group_by   ) => {
                let child_tf = self.analyze(group_by.input.clone(), plan_ctx)?;
                Ok(group_by.rebuild_or_clone(child_tf, logical_plan.clone()))
            },
            LogicalPlan::GraphNode(graph_node) => {
                let child_tf = self.analyze(graph_node.input.clone(), plan_ctx)?;
                Ok(graph_node.rebuild_or_clone(child_tf, logical_plan.clone()))
            },
            LogicalPlan::GraphRel(graph_rel) => {
                let left_tf = self.analyze(graph_rel.left.clone(), plan_ctx)?;
                let center_tf = self.analyze(graph_rel.center.clone(), plan_ctx)?;
                let right_tf = self.analyze(graph_rel.right.clone(), plan_ctx)?;
                Ok(graph_rel.rebuild_or_clone(left_tf, center_tf, right_tf, logical_plan.clone()))
            },
            LogicalPlan::Cte(cte   ) => {
                let child_tf = self.analyze( cte.input.clone(), plan_ctx)?;
                Ok(cte.rebuild_or_clone(child_tf, logical_plan.clone()))
            },
            LogicalPlan::Scan(_) => {
                Ok(Transformed::No(logical_plan.clone()))
            },
            LogicalPlan::Empty => Ok(Transformed::No(logical_plan.clone())),
            LogicalPlan::GraphJoins(graph_joins) => {
                let child_tf = self.analyze(graph_joins.input.clone(), plan_ctx)?;
                Ok(graph_joins.rebuild_or_clone(child_tf, logical_plan.clone()))
            },
            LogicalPlan::Filter(filter) => {
                let child_tf = self.analyze(filter.input.clone(), plan_ctx)?;
                Ok(filter.rebuild_or_clone(child_tf, logical_plan.clone()))
            },
            LogicalPlan::OrderBy(order_by) => {
                let child_tf = self.analyze(order_by.input.clone(), plan_ctx)?;
                Ok(order_by.rebuild_or_clone(child_tf, logical_plan.clone()))
            },
            LogicalPlan::Skip(skip) => {
                let child_tf = self.analyze(skip.input.clone(), plan_ctx)?;
                Ok(skip.rebuild_or_clone(child_tf, logical_plan.clone()))
            },
            LogicalPlan::Limit(limit) => {
                let child_tf = self.analyze(limit.input.clone(), plan_ctx)?;
                Ok(limit.rebuild_or_clone(child_tf, logical_plan.clone()))
            },
        }
    }
}

impl GroupByBuilding {
    pub fn new() -> Self {
        GroupByBuilding
    }
}

