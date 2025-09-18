use std::sync::Arc;

use crate::query_planner::{
    logical_expr::logical_expr::{LogicalExpr, Operator, OperatorApplication},
    logical_plan::logical_plan::{Filter, LogicalPlan},
    optimizer::{
        errors::{OptimizerError, Pass},
        optimizer_pass::{OptimizerPass, OptimizerResult},
    },
    plan_ctx::plan_ctx::PlanCtx,
    transformed::Transformed,
};

pub struct FilterPushDown;

impl OptimizerPass for FilterPushDown {
    fn optimize(
        &self,
        logical_plan: Arc<LogicalPlan>,
        plan_ctx: &mut PlanCtx,
    ) -> OptimizerResult<Transformed<Arc<LogicalPlan>>> {
        let transformed_plan = match logical_plan.as_ref() {
            LogicalPlan::GraphNode(graph_node) => {
                let child_tf = self.optimize(graph_node.input.clone(), plan_ctx)?;
                graph_node.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::GraphRel(graph_rel) => {
                let left_tf = self.optimize(graph_rel.left.clone(), plan_ctx)?;
                let center_tf = self.optimize(graph_rel.center.clone(), plan_ctx)?;
                let right_tf = self.optimize(graph_rel.right.clone(), plan_ctx)?;
                graph_rel.rebuild_or_clone(left_tf, center_tf, right_tf, logical_plan.clone())
            }
            LogicalPlan::Cte(cte) => {
                let child_tf = self.optimize(cte.input.clone(), plan_ctx)?;
                cte.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Scan(scan) => {
                let table_ctx_opt = plan_ctx
                    .get_mut_table_ctx_opt_from_alias_opt(&scan.table_alias)
                    .map_err(|e| OptimizerError::PlanCtx {
                        pass: Pass::FilterPushDown,
                        source: e,
                    })?;
                if let Some(table_ctx) = table_ctx_opt {
                    if !table_ctx.get_filters().is_empty() {
                        let combined_predicate = self
                            .get_combined_predicate(table_ctx.get_filters().clone())
                            .ok_or(OptimizerError::CombineFilterPredicate)?;

                        let new_proj = Arc::new(LogicalPlan::Filter(Filter {
                            input: logical_plan.clone(),
                            predicate: combined_predicate,
                        }));
                        return Ok(Transformed::Yes(new_proj));
                    }
                }
                Transformed::No(logical_plan.clone())
            }
            LogicalPlan::Empty => Transformed::No(logical_plan.clone()),
            LogicalPlan::GraphJoins(graph_joins) => {
                let child_tf = self.optimize(graph_joins.input.clone(), plan_ctx)?;
                graph_joins.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Filter(filter) => {
                let child_tf = self.optimize(filter.input.clone(), plan_ctx)?;
                filter.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Projection(projection) => {
                let child_tf = self.optimize(projection.input.clone(), plan_ctx)?;
                projection.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::GroupBy(group_by) => {
                let child_tf = self.optimize(group_by.input.clone(), plan_ctx)?;
                group_by.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::OrderBy(order_by) => {
                let child_tf = self.optimize(order_by.input.clone(), plan_ctx)?;
                order_by.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Skip(skip) => {
                let child_tf = self.optimize(skip.input.clone(), plan_ctx)?;
                skip.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Limit(limit) => {
                let child_tf = self.optimize(limit.input.clone(), plan_ctx)?;
                limit.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Union(union) => {
                let mut inputs_tf: Vec<Transformed<Arc<LogicalPlan>>> = vec![];
                for input_plan in union.inputs.iter() {
                    let child_tf = self.optimize(input_plan.clone(), plan_ctx)?;
                    inputs_tf.push(child_tf);
                }
                union.rebuild_or_clone(inputs_tf, logical_plan.clone())
            }
        };
        Ok(transformed_plan)
    }
}

impl FilterPushDown {
    pub fn new() -> Self {
        FilterPushDown
    }

    pub fn get_combined_predicate(&self, filter_items: Vec<LogicalExpr>) -> Option<LogicalExpr> {
        let mut iter = filter_items.into_iter();
        let first = iter.next();

        first.map(|first_expr| {
            iter.fold(first_expr, |acc, expr| {
                LogicalExpr::OperatorApplicationExp(OperatorApplication {
                    operator: Operator::And,
                    operands: vec![acc, expr],
                })
            })
        })
    }
}
