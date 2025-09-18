use std::sync::Arc;

use crate::query_planner::{
    logical_expr::{LogicalExpr, Operator},
    logical_plan::{GraphRel, LogicalPlan},
    optimizer::{
        errors::OptimizerError,
        optimizer_pass::{OptimizerPass, OptimizerResult},
    },
    plan_ctx::PlanCtx,
    transformed::Transformed,
};

pub struct AnchorNodeSelection;

impl OptimizerPass for AnchorNodeSelection {
    fn optimize(
        &self,
        logical_plan: Arc<LogicalPlan>,
        plan_ctx: &mut PlanCtx,
    ) -> OptimizerResult<Transformed<Arc<LogicalPlan>>> {
        if let Some(anchor_node_alias) = self.find_anchor_node(plan_ctx) {
            return Self::anchor_traversal(anchor_node_alias, logical_plan);
        }

        Ok(Transformed::No(logical_plan))
    }
}

impl AnchorNodeSelection {
    pub fn new() -> Self {
        AnchorNodeSelection
    }

    // Get anchor node with max number of filters. If there is a tie, then check for any filter with OR operator.
    // If there is no such filter, then return any one of the matched
    // TODO: Should we also check for other condtion precedence? Like IN, NOT IN, etc.
    fn find_anchor_node(&self, plan_ctx: &PlanCtx) -> Option<String> {
        let mut max_filter_count = 0;
        let mut candidates = Vec::new();

        // find tables with maximum number of filters
        for (alias, table_ctx) in plan_ctx.get_alias_table_ctx_map() {
            let filter_count = table_ctx.get_filters().len();

            if filter_count > max_filter_count {
                max_filter_count = filter_count;
                candidates.clear();
                candidates.push(alias.clone());
            } else if filter_count == max_filter_count && filter_count > 0 {
                candidates.push(alias.clone());
            }
        }

        // If no table has filters, return None
        if max_filter_count == 0 {
            return None;
        }

        // If only one candidate, return it
        if candidates.len() == 1 {
            return Some(candidates[0].clone());
        }

        // If multiple candidates (tie), check for OR operators
        for candidate in &candidates {
            if let Ok(table_ctx) = plan_ctx.get_table_ctx(candidate) {
                for filter in table_ctx.get_filters() {
                    if Self::has_or_operator(filter) {
                        return Some(candidate.clone());
                    }
                }
            }
        }

        // If no OR operator found, return the first candidate
        candidates.into_iter().next()
    }

    // check for OR operators in expressions
    fn has_or_operator(expr: &LogicalExpr) -> bool {
        match expr {
            LogicalExpr::OperatorApplicationExp(op_app) => {
                if op_app.operator == Operator::Or {
                    return true;
                }
                // check operands for nested OR conditions
                for operand in &op_app.operands {
                    if Self::has_or_operator(operand) {
                        return true;
                    }
                }
                false
            }
            LogicalExpr::ScalarFnCall(fc) => {
                for arg in &fc.args {
                    if Self::has_or_operator(arg) {
                        return true;
                    }
                }
                false
            }
            LogicalExpr::AggregateFnCall(fc) => {
                for arg in &fc.args {
                    if Self::has_or_operator(arg) {
                        return true;
                    }
                }
                false
            }
            LogicalExpr::List(exprs) => {
                for expr in exprs {
                    if Self::has_or_operator(expr) {
                        return true;
                    }
                }
                false
            }
            _ => false,
        }
    }

    fn anchor_traversal(
        anchor_node_alias: String,
        logical_plan: Arc<LogicalPlan>
    ) -> OptimizerResult<Transformed<Arc<LogicalPlan>>> {
        let transformed_plan = match logical_plan.as_ref() {
            LogicalPlan::GraphNode(graph_node) => {
                let child_tf = Self::anchor_traversal(
                    anchor_node_alias.clone(),
                    graph_node.input.clone(),
                )?;
                // let self_tf = self.anchor_traversal(anchor_node_alias, graph_node.self_plan.clone(), plan_ctx);
                graph_node.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::GraphRel(graph_rel) => {
                // if anchor node found at right side then it means we have found it at the end of the graph traversal. It is already a start node.

                // If found at left then we need to create a new plan and rotate the right side.
                if graph_rel.left_connection == anchor_node_alias {
                    let new_anchor_plan = Arc::new(LogicalPlan::GraphRel(GraphRel {
                        left: Arc::new(LogicalPlan::Empty),
                        center: graph_rel.center.clone(),
                        right: graph_rel.left.clone(),
                        alias: graph_rel.alias.clone(),
                        direction: graph_rel.direction.clone().reverse(),
                        // as we are rotating the nodes, we will rotate the connections as well
                        left_connection: graph_rel.right_connection.clone(),
                        right_connection: graph_rel.left_connection.clone(),
                        is_rel_anchor: false,
                    }));
                    let rotated_plan =
                        Self::rotate_plan(new_anchor_plan, graph_rel.right.clone())?;

                    Transformed::Yes(rotated_plan)

                    // similarly check for anchor node at relation i.e. at center
                } else if graph_rel.alias == anchor_node_alias {
                    let new_anchor_plan = Arc::new(LogicalPlan::GraphRel(GraphRel {
                        left: Arc::new(LogicalPlan::Empty),
                        center: graph_rel.left.clone(),
                        right: graph_rel.center.clone(),
                        alias: graph_rel.alias.clone(),
                        direction: graph_rel.direction.clone().reverse(),
                        // as we are rotating the nodes, we will rotate the connections as well
                        left_connection: graph_rel.right_connection.clone(),
                        right_connection: graph_rel.left_connection.clone(),
                        is_rel_anchor: true,
                    }));
                    let rotated_plan =
                        Self::rotate_plan(new_anchor_plan, graph_rel.right.clone())?;

                    Transformed::Yes(rotated_plan)
                } else {
                    let left_tf = Self::anchor_traversal(
                        anchor_node_alias.clone(),
                        graph_rel.left.clone(),
                    )?;
                    let center_tf = Self::anchor_traversal(
                        anchor_node_alias.clone(),
                        graph_rel.center.clone(),
                    )?;
                    let right_tf = Self::anchor_traversal(
                        anchor_node_alias,
                        graph_rel.right.clone(),
                    )?;
                    graph_rel.rebuild_or_clone(left_tf, center_tf, right_tf, logical_plan.clone())
                }
            }
            LogicalPlan::Cte(cte) => {
                let child_tf =
                    Self::anchor_traversal(anchor_node_alias, cte.input.clone())?;
                cte.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Scan(_) => Transformed::No(logical_plan.clone()),
            LogicalPlan::Empty => Transformed::No(logical_plan.clone()),
            LogicalPlan::GraphJoins(graph_joins) => {
                let child_tf =
                    Self::anchor_traversal(anchor_node_alias, graph_joins.input.clone())?;
                graph_joins.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Filter(filter) => {
                let child_tf =
                    Self::anchor_traversal(anchor_node_alias, filter.input.clone())?;
                filter.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Projection(projection) => {
                let child_tf =
                    Self::anchor_traversal(anchor_node_alias, projection.input.clone())?;
                projection.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::GroupBy(group_by) => {
                let child_tf =
                    Self::anchor_traversal(anchor_node_alias, group_by.input.clone())?;
                group_by.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::OrderBy(order_by) => {
                let child_tf =
                    Self::anchor_traversal(anchor_node_alias, order_by.input.clone())?;
                order_by.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Skip(skip) => {
                let child_tf =
                    Self::anchor_traversal(anchor_node_alias, skip.input.clone())?;
                skip.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Limit(limit) => {
                let child_tf =
                    Self::anchor_traversal(anchor_node_alias, limit.input.clone())?;
                limit.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Union(union) => {
                let mut inputs_tf: Vec<Transformed<Arc<LogicalPlan>>> = vec![];
                for input_plan in union.inputs.iter() {
                    let child_tf = Self::anchor_traversal(
                        anchor_node_alias.clone(),
                        input_plan.clone(),
                    )?;
                    inputs_tf.push(child_tf);
                }
                union.rebuild_or_clone(inputs_tf, logical_plan.clone())
            }
        };
        Ok(transformed_plan)
    }

    fn rotate_plan(
        new_plan: Arc<LogicalPlan>,
        remaining_plan: Arc<LogicalPlan>,
    ) -> OptimizerResult<Arc<LogicalPlan>> {
        match remaining_plan.as_ref() {
            LogicalPlan::GraphNode(graph_node) => {
                if let LogicalPlan::GraphRel(prev_graph_rel) = new_plan.as_ref() {
                    let new_constructed_plan = Arc::new(LogicalPlan::GraphRel(GraphRel {
                        left: Arc::new(LogicalPlan::GraphNode(graph_node.clone())),
                        center: prev_graph_rel.center.clone(),
                        right: prev_graph_rel.right.clone(),
                        alias: prev_graph_rel.alias.clone(),
                        direction: prev_graph_rel.direction.clone(),
                        left_connection: graph_node.alias.clone(),
                        right_connection: prev_graph_rel.right_connection.clone(),
                        is_rel_anchor: prev_graph_rel.is_rel_anchor,
                    }));
                    return Ok(new_constructed_plan);
                }
                Err(OptimizerError::MissingGraphRelInRotatePlan)
            }
            LogicalPlan::GraphRel(graph_rel) => {
                if let LogicalPlan::GraphRel(prev_graph_rel) = new_plan.as_ref() {
                    // check how the prev graph is connected to this current one
                    // We can do that by checking prev graph's left connected to current graph's left or right
                    let (prev_left, new_remaining) =
                        if prev_graph_rel.left_connection == graph_rel.left_connection {
                            (graph_rel.left.clone(), graph_rel.right.clone())
                        } else {
                            (graph_rel.right.clone(), graph_rel.left.clone())
                        };

                    let new_constructed_plan = Arc::new(LogicalPlan::GraphRel(GraphRel {
                        left: Arc::new(LogicalPlan::Empty),
                        center: graph_rel.center.clone(),
                        right: Arc::new(LogicalPlan::GraphRel(GraphRel {
                            left: prev_left,
                            center: prev_graph_rel.center.clone(),
                            right: prev_graph_rel.right.clone(),
                            alias: prev_graph_rel.alias.clone(),
                            direction: prev_graph_rel.direction.clone(),
                            left_connection: prev_graph_rel.left_connection.clone(),
                            right_connection: prev_graph_rel.right_connection.clone(),
                            is_rel_anchor: prev_graph_rel.is_rel_anchor,
                        })),
                        alias: graph_rel.alias.clone(),
                        direction: graph_rel.direction.clone(), //.reverse(),
                        // We don't need to rotate the left_conn and right_conn as we have done it at the anchor node.
                        // Here we are respecting the connection pattern
                        left_connection: graph_rel.left_connection.clone(),
                        right_connection: graph_rel.right_connection.clone(),
                        is_rel_anchor: false,
                    }));

                    return Self::rotate_plan(new_constructed_plan, new_remaining);
                }

                Err(OptimizerError::MissingGraphRelInRotatePlan)
            }
            _ => Ok(new_plan.clone()),
        }
    }
}
