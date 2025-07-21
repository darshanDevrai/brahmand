use std::sync::Arc;

use crate::query_planner::{logical_expr::logical_expr::{Operator, OperatorApplication, LogicalExpr}, logical_plan::logical_plan::{Filter, LogicalPlan}, optimizer::optimizer_pass::OptimizerPass, plan_ctx::plan_ctx::PlanCtx, transformed::Transformed};




pub struct FilterPushDown;


impl OptimizerPass for FilterPushDown {
    fn optimize(&self, logical_plan: Arc<LogicalPlan>, plan_ctx: &mut PlanCtx) -> Transformed<Arc<LogicalPlan>> {
        match logical_plan.as_ref() {
            LogicalPlan::GraphNode(graph_node) => {
                let child_tf = self.optimize(graph_node.input.clone(), plan_ctx);
                graph_node.rebuild_or_clone(child_tf, logical_plan.clone())
            },
            LogicalPlan::GraphRel(graph_rel) => {
                let left_tf = self.optimize(graph_rel.left.clone(), plan_ctx);
                let center_tf = self.optimize(graph_rel.center.clone(), plan_ctx);
                let right_tf = self.optimize(graph_rel.right.clone(), plan_ctx);
                graph_rel.rebuild_or_clone(left_tf, center_tf, right_tf, logical_plan.clone())
            },
            LogicalPlan::Cte(cte   ) => {
                let child_tf = self.optimize( cte.input.clone(), plan_ctx);
                cte.rebuild_or_clone(child_tf, logical_plan.clone())
            },
            LogicalPlan::Scan(scan) => {
                        if let Some(table_ctx) = plan_ctx.alias_table_ctx_map.get_mut(&scan.table_alias) {
                            if !table_ctx.filter_predicates.is_empty() {

                                let combined_predicate = self.get_combined_predicate(table_ctx.filter_predicates.clone()).unwrap();

                                let new_proj = Arc::new(LogicalPlan::Filter(Filter {
                                    input: logical_plan.clone(),
                                    predicate: combined_predicate
                                }));
                                return Transformed::Yes(new_proj)
                            }
                        }
                        Transformed::No(logical_plan.clone())
                    },
            LogicalPlan::Empty => Transformed::No(logical_plan.clone()),
            LogicalPlan::GraphJoins(graph_joins) => {
                        let child_tf = self.optimize(graph_joins.input.clone(), plan_ctx);
                        graph_joins.rebuild_or_clone(child_tf, logical_plan.clone())
                    },
            LogicalPlan::Filter(filter) => {
                        let child_tf = self.optimize(filter.input.clone(), plan_ctx);
                        filter.rebuild_or_clone(child_tf, logical_plan.clone())
                    },
            LogicalPlan::Projection(projection) => {
                        let child_tf = self.optimize(projection.input.clone(), plan_ctx);
                        projection.rebuild_or_clone(child_tf, logical_plan.clone())
                    },
            LogicalPlan::GroupBy(group_by   ) => {
                        let child_tf = self.optimize(group_by.input.clone(), plan_ctx);
                        group_by.rebuild_or_clone(child_tf, logical_plan.clone())
                    },
            LogicalPlan::OrderBy(order_by) => {
                        let child_tf = self.optimize(order_by.input.clone(), plan_ctx);
                        order_by.rebuild_or_clone(child_tf, logical_plan.clone())
                    },
            LogicalPlan::Skip(skip) => {
                        let child_tf = self.optimize(skip.input.clone(), plan_ctx);
                        skip.rebuild_or_clone(child_tf, logical_plan.clone())
                    },
            LogicalPlan::Limit(limit) => {
                        let child_tf = self.optimize(limit.input.clone(), plan_ctx);
                        limit.rebuild_or_clone(child_tf, logical_plan.clone())
                    },
            
        }
    }
}


impl FilterPushDown {
    pub fn new() -> Self {
        FilterPushDown
    }

    pub fn get_combined_predicate(&self, filter_items: Vec<LogicalExpr>) -> Option<LogicalExpr> {
        let mut iter = filter_items.into_iter();
        let first = iter.next();

        let combined = first.map(|first_expr| {
            iter.fold(first_expr, |acc, expr| {
                LogicalExpr::OperatorApplicationExp(OperatorApplication {
                    operator: Operator::And,
                    operands: vec![acc, expr],
                })
            })
        });

        combined
    }
}