use std::sync::Arc;

use crate::query_planner::{
    analyzer::analyzer_pass::{AnalyzerPass, AnalyzerResult},
    logical_expr::LogicalExpr,
    logical_plan::{GroupBy, LogicalPlan, ProjectionItem},
    plan_ctx::PlanCtx,
    transformed::Transformed,
};

pub struct GroupByBuilding;

// In the final projections, if there is an aggregate fn then add other projections in group by clause
// build group by plan after projection tagging.
impl AnalyzerPass for GroupByBuilding {
    fn analyze(
        &self,
        logical_plan: Arc<LogicalPlan>,
        _plan_ctx: &mut PlanCtx,
    ) -> AnalyzerResult<Transformed<Arc<LogicalPlan>>> {
        let transformed_plan = match logical_plan.as_ref() {
            LogicalPlan::Projection(projection) => {
                let non_agg_projections: Vec<ProjectionItem> = projection
                    .items
                    .iter()
                    .filter(|item| !matches!(item.expression, LogicalExpr::AggregateFnCall(_)))
                    .cloned()
                    .collect();

                if non_agg_projections.len() < projection.items.len()
                    && !non_agg_projections.is_empty()
                {
                    // aggregate fns found. Build the groupby plan here
                    Transformed::Yes(Arc::new(LogicalPlan::GroupBy(GroupBy {
                        input: logical_plan.clone(),
                        expressions: non_agg_projections
                            .into_iter()
                            .map(|item| item.expression)
                            .collect(),
                    })))
                } else {
                    let child_tf = self.analyze(projection.input.clone(), _plan_ctx)?;
                    projection.rebuild_or_clone(child_tf, logical_plan.clone())
                }
            }
            LogicalPlan::GroupBy(group_by) => {
                let child_tf = self.analyze(group_by.input.clone(), _plan_ctx)?;
                group_by.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::GraphNode(graph_node) => {
                let child_tf = self.analyze(graph_node.input.clone(), _plan_ctx)?;
                graph_node.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::GraphRel(graph_rel) => {
                let left_tf = self.analyze(graph_rel.left.clone(), _plan_ctx)?;
                let center_tf = self.analyze(graph_rel.center.clone(), _plan_ctx)?;
                let right_tf = self.analyze(graph_rel.right.clone(), _plan_ctx)?;
                graph_rel.rebuild_or_clone(left_tf, center_tf, right_tf, logical_plan.clone())
            }
            LogicalPlan::Cte(cte) => {
                let child_tf = self.analyze(cte.input.clone(), _plan_ctx)?;
                cte.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Scan(_) => Transformed::No(logical_plan.clone()),
            LogicalPlan::Empty => Transformed::No(logical_plan.clone()),
            LogicalPlan::GraphJoins(graph_joins) => {
                let child_tf = self.analyze(graph_joins.input.clone(), _plan_ctx)?;
                graph_joins.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Filter(filter) => {
                let child_tf = self.analyze(filter.input.clone(), _plan_ctx)?;
                filter.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::OrderBy(order_by) => {
                let child_tf = self.analyze(order_by.input.clone(), _plan_ctx)?;
                order_by.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Skip(skip) => {
                let child_tf = self.analyze(skip.input.clone(), _plan_ctx)?;
                skip.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Limit(limit) => {
                let child_tf = self.analyze(limit.input.clone(), _plan_ctx)?;
                limit.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Union(union) => {
                let mut inputs_tf: Vec<Transformed<Arc<LogicalPlan>>> = vec![];
                for input_plan in union.inputs.iter() {
                    let child_tf = self.analyze(input_plan.clone(), _plan_ctx)?;
                    inputs_tf.push(child_tf);
                }
                union.rebuild_or_clone(inputs_tf, logical_plan.clone())
            }
        };
        Ok(transformed_plan)
    }
}

impl GroupByBuilding {
    pub fn new() -> Self {
        GroupByBuilding
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_planner::logical_expr::{
        AggregateFnCall, Column, PropertyAccess, TableAlias,
    };
    use crate::query_planner::logical_plan::{LogicalPlan, Projection, Scan};

    fn create_property_access(table: &str, column: &str) -> LogicalExpr {
        LogicalExpr::PropertyAccessExp(PropertyAccess {
            table_alias: TableAlias(table.to_string()),
            column: Column(column.to_string()),
        })
    }

    fn create_aggregate_function(name: &str, arg_table: &str, arg_column: &str) -> LogicalExpr {
        LogicalExpr::AggregateFnCall(AggregateFnCall {
            name: name.to_string(),
            args: vec![create_property_access(arg_table, arg_column)],
        })
    }

    fn create_scan(alias: Option<String>, table_name: Option<String>) -> Arc<LogicalPlan> {
        Arc::new(LogicalPlan::Scan(Scan {
            table_alias: alias,
            table_name: table_name,
        }))
    }

    #[test]
    fn test_projection_with_mixed_aggregate_and_non_aggregate() {
        let analyzer = GroupByBuilding::new();
        let mut plan_ctx = PlanCtx::default();

        // Test projection: SELECT user.name, COUNT(order.id) FROM ...
        let scan = create_scan(Some("user".to_string()), Some("users".to_string()));
        let projection = Arc::new(LogicalPlan::Projection(Projection {
            input: scan,
            items: vec![
                ProjectionItem {
                    expression: create_property_access("user", "name"),
                    col_alias: None,
                },
                ProjectionItem {
                    expression: create_aggregate_function("count", "order", "id"),
                    col_alias: None,
                },
            ],
        }));

        let result = analyzer.analyze(projection.clone(), &mut plan_ctx).unwrap();

        // Should create GroupBy plan wrapping the projection
        match result {
            Transformed::Yes(new_plan) => {
                match new_plan.as_ref() {
                    LogicalPlan::GroupBy(group_by) => {
                        // GroupBy should wrap the original projection
                        assert_eq!(group_by.input, projection);

                        // Group expressions should contain only non-aggregate expressions
                        assert_eq!(group_by.expressions.len(), 1);
                        match &group_by.expressions[0] {
                            LogicalExpr::PropertyAccessExp(prop_acc) => {
                                assert_eq!(prop_acc.table_alias.0, "user");
                                assert_eq!(prop_acc.column.0, "name");
                            }
                            _ => panic!("Expected PropertyAccess in group expressions"),
                        }
                    }
                    _ => panic!("Expected GroupBy plan"),
                }
            }
            _ => panic!("Expected transformation"),
        }
    }

    #[test]
    fn test_projection_with_only_aggregates_no_groupby() {
        let analyzer = GroupByBuilding::new();
        let mut plan_ctx = PlanCtx::default();

        // Test projection: SELECT COUNT(order.id), SUM(order.amount) FROM ...
        let scan = create_scan(Some("order".to_string()), Some("orders".to_string()));
        let projection = Arc::new(LogicalPlan::Projection(Projection {
            input: scan,
            items: vec![
                ProjectionItem {
                    expression: create_aggregate_function("count", "order", "id"),
                    col_alias: None,
                },
                ProjectionItem {
                    expression: create_aggregate_function("sum", "order", "amount"),
                    col_alias: None,
                },
            ],
        }));

        let result = analyzer.analyze(projection.clone(), &mut plan_ctx).unwrap();

        // Should NOT create GroupBy (only aggregates, no grouping needed)
        match result {
            Transformed::No(plan) => {
                assert_eq!(plan, projection); // Should return original plan unchanged
            }
            _ => panic!("Expected no transformation for aggregates-only projection"),
        }
    }

    #[test]
    fn test_projection_with_only_non_aggregates_no_groupby() {
        let analyzer = GroupByBuilding::new();
        let mut plan_ctx = PlanCtx::default();

        // Test projection: SELECT user.name, user.email FROM ...
        let scan = create_scan(Some("user".to_string()), Some("users".to_string()));
        let projection = Arc::new(LogicalPlan::Projection(Projection {
            input: scan,
            items: vec![
                ProjectionItem {
                    expression: create_property_access("user", "name"),
                    col_alias: None,
                },
                ProjectionItem {
                    expression: create_property_access("user", "email"),
                    col_alias: None,
                },
            ],
        }));

        let result = analyzer.analyze(projection.clone(), &mut plan_ctx).unwrap();

        // Should NOT create GroupBy (no aggregates present)
        match result {
            Transformed::No(plan) => {
                assert_eq!(plan, projection); // Should return original plan unchanged
            }
            _ => panic!("Expected no transformation for non-aggregates-only projection"),
        }
    }

    #[test]
    fn test_projection_with_multiple_non_aggregates_and_aggregate() {
        let analyzer = GroupByBuilding::new();
        let mut plan_ctx = PlanCtx::default();

        // Test projection: SELECT user.name, user.city, COUNT(order.id) FROM ...
        let scan = create_scan(Some("user".to_string()), Some("users".to_string()));
        let projection = Arc::new(LogicalPlan::Projection(Projection {
            input: scan,
            items: vec![
                ProjectionItem {
                    expression: create_property_access("user", "name"),
                    col_alias: None,
                },
                ProjectionItem {
                    expression: create_property_access("user", "city"),
                    col_alias: None,
                },
                ProjectionItem {
                    expression: create_aggregate_function("count", "order", "id"),
                    col_alias: None,
                },
            ],
        }));

        let result = analyzer.analyze(projection.clone(), &mut plan_ctx).unwrap();

        // Should create GroupBy with both non-aggregate expressions
        match result {
            Transformed::Yes(new_plan) => {
                match new_plan.as_ref() {
                    LogicalPlan::GroupBy(group_by) => {
                        assert_eq!(group_by.expressions.len(), 2);

                        // First group expression: user.name
                        match &group_by.expressions[0] {
                            LogicalExpr::PropertyAccessExp(prop_acc) => {
                                assert_eq!(prop_acc.column.0, "name");
                            }
                            _ => panic!("Expected PropertyAccess"),
                        }

                        // Second group expression: user.city
                        match &group_by.expressions[1] {
                            LogicalExpr::PropertyAccessExp(prop_acc) => {
                                assert_eq!(prop_acc.column.0, "city");
                            }
                            _ => panic!("Expected PropertyAccess"),
                        }
                    }
                    _ => panic!("Expected GroupBy plan"),
                }
            }
            _ => panic!("Expected transformation"),
        }
    }

    #[test]
    fn test_empty_projection_no_groupby() {
        let analyzer = GroupByBuilding::new();
        let mut plan_ctx = PlanCtx::default();

        // Test empty projection
        let scan = create_scan(Some("user".to_string()), Some("users".to_string()));
        let projection = Arc::new(LogicalPlan::Projection(Projection {
            input: scan,
            items: vec![],
        }));

        let result = analyzer.analyze(projection.clone(), &mut plan_ctx).unwrap();

        // Should NOT create GroupBy (empty projection)
        match result {
            Transformed::No(plan) => {
                assert_eq!(plan, projection);
            }
            _ => panic!("Expected no transformation for empty projection"),
        }
    }
}
