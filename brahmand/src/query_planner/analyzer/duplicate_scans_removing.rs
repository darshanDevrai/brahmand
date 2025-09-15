use std::{collections::HashSet, sync::Arc};

use crate::query_planner::{
    analyzer::analyzer_pass::{AnalyzerPass, AnalyzerResult},
    logical_plan::logical_plan::LogicalPlan,
    plan_ctx::plan_ctx::PlanCtx,
    transformed::Transformed,
};

pub struct DuplicateScansRemoving;

impl AnalyzerPass for DuplicateScansRemoving {
    fn analyze(
        &self,
        logical_plan: Arc<LogicalPlan>,
        _: &mut PlanCtx,
    ) -> AnalyzerResult<Transformed<Arc<LogicalPlan>>> {
        let mut traversed: HashSet<String> = HashSet::new();
        self.remove_duplicate_scans(logical_plan, &mut traversed)
    }
}

impl DuplicateScansRemoving {
    pub fn new() -> Self {
        DuplicateScansRemoving
    }

    fn remove_duplicate_scans(
        &self,
        logical_plan: Arc<LogicalPlan>,
        traversed: &mut HashSet<String>,
    ) -> AnalyzerResult<Transformed<Arc<LogicalPlan>>> {
        let transformed_plan = match logical_plan.as_ref() {
            LogicalPlan::Projection(projection) => {
                let child_tf = self.remove_duplicate_scans(projection.input.clone(), traversed)?;
                projection.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::GraphNode(graph_node) => {
                traversed.insert(graph_node.alias.clone());

                let child_tf = self.remove_duplicate_scans(graph_node.input.clone(), traversed)?;
                // let self_tf = self.remove_duplicate_scans(graph_node.self_plan.clone(), traversed);
                graph_node.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::GraphRel(graph_rel) => {
                let right_tf = self.remove_duplicate_scans(graph_rel.right.clone(), traversed)?;
                let center_tf = self.remove_duplicate_scans(graph_rel.center.clone(), traversed)?;

                let left_alias = &graph_rel.left_connection;

                let left_tf = if traversed.contains(left_alias) {
                    Transformed::Yes(Arc::new(LogicalPlan::Empty))
                } else {
                    self.remove_duplicate_scans(graph_rel.left.clone(), traversed)?
                };

                // let left_tf = self.remove_duplicate_scans(graph_rel.left.clone(), traversed);

                graph_rel.rebuild_or_clone(left_tf, center_tf, right_tf, logical_plan.clone())
            }
            LogicalPlan::Cte(cte) => {
                let child_tf = self.remove_duplicate_scans(cte.input.clone(), traversed)?;
                cte.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Scan(_) => Transformed::No(logical_plan.clone()),
            LogicalPlan::Empty => Transformed::No(logical_plan.clone()),
            LogicalPlan::GraphJoins(graph_joins) => {
                let child_tf = self.remove_duplicate_scans(graph_joins.input.clone(), traversed)?;
                graph_joins.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Filter(filter) => {
                let child_tf = self.remove_duplicate_scans(filter.input.clone(), traversed)?;
                filter.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::GroupBy(group_by) => {
                let child_tf = self.remove_duplicate_scans(group_by.input.clone(), traversed)?;
                group_by.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::OrderBy(order_by) => {
                let child_tf = self.remove_duplicate_scans(order_by.input.clone(), traversed)?;
                order_by.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Skip(skip) => {
                let child_tf = self.remove_duplicate_scans(skip.input.clone(), traversed)?;
                skip.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Limit(limit) => {
                let child_tf = self.remove_duplicate_scans(limit.input.clone(), traversed)?;
                limit.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Union(union) => {
                let mut inputs_tf: Vec<Transformed<Arc<LogicalPlan>>> = vec![];
                for input_plan in union.inputs.iter() {
                    let child_tf = self.remove_duplicate_scans(input_plan.clone(), traversed)?;
                    inputs_tf.push(child_tf);
                }
                union.rebuild_or_clone(inputs_tf, logical_plan.clone())
            }
        };
        Ok(transformed_plan)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_planner::logical_expr::logical_expr::{
        Direction, Literal, LogicalExpr, Operator, OperatorApplication,
    };
    use crate::query_planner::logical_plan::logical_plan::{
        Filter, GraphNode, GraphRel, LogicalPlan, Projection, ProjectionItem, Scan,
    };

    // helper functions
    fn create_scan(alias: Option<String>, table_name: Option<String>) -> Arc<LogicalPlan> {
        Arc::new(LogicalPlan::Scan(Scan {
            table_alias: alias.map(|s| s.to_string()),
            table_name: table_name.map(|s| s.to_string()),
        }))
    }

    fn create_graph_node(alias: &str, input: Arc<LogicalPlan>) -> Arc<LogicalPlan> {
        Arc::new(LogicalPlan::GraphNode(GraphNode {
            input,
            alias: alias.to_string(),
        }))
    }

    fn create_graph_rel(
        left: Arc<LogicalPlan>,
        center: Arc<LogicalPlan>,
        right: Arc<LogicalPlan>,
        alias: &str,
        left_connection: &str,
        right_connection: &str,
        direction: Direction,
    ) -> Arc<LogicalPlan> {
        Arc::new(LogicalPlan::GraphRel(GraphRel {
            left,
            center,
            right,
            alias: alias.to_string(),
            direction,
            left_connection: left_connection.to_string(),
            right_connection: right_connection.to_string(),
            is_rel_anchor: false,
        }))
    }

    #[test]
    fn test_complex_nested_plan_with_duplicates() {
        let analyzer = DuplicateScansRemoving::new();
        let mut plan_ctx = PlanCtx::default();

        // Create a complex plan: Projection -> Filter -> GraphRel with duplicate detection

        let left_user_scan = create_scan(Some("user".to_string()), Some("users".to_string()));
        let left_user_node = create_graph_node("user", left_user_scan);

        let center_scan = create_scan(
            Some("follows".to_string()),
            Some("follows_table".to_string()),
        );

        let right_user_scan = create_scan(Some("user".to_string()), Some("users".to_string())); // Duplicate of left
        let right_user_node = create_graph_node("user", right_user_scan);

        let graph_rel = create_graph_rel(
            right_user_node, // This should be replaced with Empty
            center_scan,
            left_user_node, // This traverses "user" first
            "follows",
            "user", // left_connection matches traversed alias
            "user", // right_connection
            Direction::Either,
        );

        // Wrap in Filter
        let filter = Arc::new(LogicalPlan::Filter(Filter {
            input: graph_rel,
            predicate: LogicalExpr::OperatorApplicationExp(OperatorApplication {
                operator: Operator::GreaterThan,
                operands: vec![
                    LogicalExpr::Literal(Literal::Integer(1)),
                    LogicalExpr::Literal(Literal::Integer(0)),
                ],
            }),
        }));

        // Wrap in Projection
        let projection = Arc::new(LogicalPlan::Projection(Projection {
            input: filter,
            items: vec![ProjectionItem {
                expression: LogicalExpr::Literal(Literal::String("test".to_string())),
                col_alias: None,
            }],
        }));

        let result = analyzer.analyze(projection, &mut plan_ctx).unwrap();

        match result {
            Transformed::Yes(transformed_plan) => {
                match transformed_plan.as_ref() {
                    LogicalPlan::Projection(proj) => {
                        match proj.input.as_ref() {
                            LogicalPlan::Filter(filter) => {
                                match filter.input.as_ref() {
                                    LogicalPlan::GraphRel(rel) => {
                                        // Left side should be Empty due to duplicate
                                        match rel.left.as_ref() {
                                            LogicalPlan::Empty => (), // Expected
                                            _ => panic!(
                                                "Expected left side to be Empty due to duplicate"
                                            ),
                                        }
                                    }
                                    _ => panic!("Expected GraphRel in filter"),
                                }
                            }
                            _ => panic!("Expected Filter in projection"),
                        }
                    }
                    _ => panic!("Expected Projection at top"),
                }
            }
            _ => panic!("Expected transformation"),
        }
    }

    #[test]
    fn test_traversed_set_tracking() {
        let analyzer = DuplicateScansRemoving::new();
        let mut plan_ctx = PlanCtx::default();

        // Create a plan that will exercise the traversed set logic
        // Structure: (userA)-[rel1]->(userB)-[rel2]->(userA)
        // The second userA should be detected as duplicate

        let user_a_scan = create_scan(Some("userA".to_string()), Some("users".to_string()));
        let user_a_node = create_graph_node("userA", user_a_scan);

        let user_b_scan = create_scan(Some("userB".to_string()), Some("users".to_string()));
        let user_b_node = create_graph_node("userB", user_b_scan);

        let rel1_scan = create_scan(Some("follows".to_string()), Some("follows".to_string()));

        // First relationship: (userA)-[follows]->(userB)
        let first_rel = create_graph_rel(
            user_a_node,
            rel1_scan,
            user_b_node,
            "follows",
            "userA",
            "userB",
            Direction::Outgoing,
        );

        // Second relationship involving userA again (should detect duplicate)
        let duplicate_user_a_scan =
            create_scan(Some("userA".to_string()), Some("users".to_string()));
        let duplicate_user_a_node = create_graph_node("userA", duplicate_user_a_scan);

        let rel2_scan = create_scan(Some("likes".to_string()), Some("likes".to_string()));

        let second_rel = create_graph_rel(
            duplicate_user_a_node, // This should become Empty
            rel2_scan,
            first_rel, // This registers userA and userB
            "likes",
            "userA", // Duplicate connection
            "userB",
            Direction::Either,
        );

        let result = analyzer.analyze(second_rel, &mut plan_ctx).unwrap();

        match result {
            Transformed::Yes(transformed_plan) => {
                match transformed_plan.as_ref() {
                    LogicalPlan::GraphRel(rel) => {
                        // Left side should be Empty due to userA duplicate
                        assert!(matches!(rel.left.as_ref(), LogicalPlan::Empty));
                        assert_eq!(rel.left_connection, "userA");
                    }
                    _ => panic!("Expected GraphRel"),
                }
            }
            _ => panic!("Expected transformation due to duplicate userA"),
        }
    }
}
