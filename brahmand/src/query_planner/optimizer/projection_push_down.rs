use std::sync::Arc;


use crate::query_planner::{logical_plan::logical_plan::{LogicalPlan, Projection}, optimizer::optimizer_pass::{OptimizerPass, OptimizerResult}, plan_ctx::plan_ctx::PlanCtx, transformed::Transformed};

pub struct ProjectionPushDown;

impl OptimizerPass for ProjectionPushDown {
    fn optimize(&self, logical_plan: Arc<LogicalPlan>, plan_ctx: &mut PlanCtx) -> OptimizerResult<Transformed<Arc<LogicalPlan>>> {
        match logical_plan.as_ref() {
            LogicalPlan::GraphNode(graph_node) => {
                let child_tf = self.optimize(graph_node.input.clone(), plan_ctx)?;
                Ok(graph_node.rebuild_or_clone(child_tf, logical_plan.clone()))
            },
            LogicalPlan::GraphRel(graph_rel) => {
                let left_tf = self.optimize(graph_rel.left.clone(), plan_ctx)?;
                let center_tf = self.optimize(graph_rel.center.clone(), plan_ctx)?;
                let right_tf = self.optimize(graph_rel.right.clone(), plan_ctx)?;
                Ok(graph_rel.rebuild_or_clone(left_tf, center_tf, right_tf, logical_plan.clone()))
            },
            LogicalPlan::Cte(cte   ) => {
                let child_tf = self.optimize( cte.input.clone(), plan_ctx)?;
                Ok(cte.rebuild_or_clone(child_tf, logical_plan.clone()))
            },
            LogicalPlan::Scan(scan) => {
                if let Some(table_ctx) = plan_ctx.alias_table_ctx_map.get_mut(&scan.table_alias) {
                    if !table_ctx.get_projections().is_empty() {
                        let new_proj = Arc::new(LogicalPlan::Projection(Projection {
                            input: logical_plan.clone(),
                            items: table_ctx.get_projections().clone(),
                        }));
                        return Ok(Transformed::Yes(new_proj))
                    }
                }
                Ok(Transformed::No(logical_plan.clone()))
            },
            LogicalPlan::Empty => Ok(Transformed::No(logical_plan.clone())),
            LogicalPlan::GraphJoins(graph_joins) => {
                let child_tf = self.optimize(graph_joins.input.clone(), plan_ctx)?;
                Ok(graph_joins.rebuild_or_clone(child_tf, logical_plan.clone()))
            },
            LogicalPlan::Filter(filter) => {
                let child_tf = self.optimize(filter.input.clone(), plan_ctx)?;
                Ok(filter.rebuild_or_clone(child_tf, logical_plan.clone()))
            },
            LogicalPlan::Projection(projection) => {
                let child_tf = self.optimize(projection.input.clone(), plan_ctx)?;
                Ok(projection.rebuild_or_clone(child_tf, logical_plan.clone()))
            },
            LogicalPlan::GroupBy(group_by   ) => {
                let child_tf = self.optimize(group_by.input.clone(), plan_ctx)?;
                Ok(group_by.rebuild_or_clone(child_tf, logical_plan.clone()))
            },
            LogicalPlan::OrderBy(order_by) => {
                let child_tf = self.optimize(order_by.input.clone(), plan_ctx)?;
                Ok(order_by.rebuild_or_clone(child_tf, logical_plan.clone()))
            },
            LogicalPlan::Skip(skip) => {
                let child_tf = self.optimize(skip.input.clone(), plan_ctx)?;
                Ok(skip.rebuild_or_clone(child_tf, logical_plan.clone()))
            },
            LogicalPlan::Limit(limit) => {
                let child_tf = self.optimize(limit.input.clone(), plan_ctx)?;
                Ok(limit.rebuild_or_clone(child_tf, logical_plan.clone()))
            },
        }
        
    }
}


impl ProjectionPushDown {

    pub fn new() -> Self { 
        ProjectionPushDown 
    }
    
}

