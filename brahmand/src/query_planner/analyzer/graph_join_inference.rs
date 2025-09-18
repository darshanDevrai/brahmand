use std::{collections::HashSet, sync::Arc};

use crate::{
    graph_catalog::graph_schema::GraphSchema,
    query_planner::{
        analyzer::{
            analyzer_pass::{AnalyzerPass, AnalyzerResult},
            errors::Pass,
            graph_context::{self, GraphContext},
        },
        logical_expr::{
            Column, Direction, LogicalExpr, Operator, OperatorApplication, PropertyAccess,
            TableAlias,
        },
        logical_plan::{GraphJoins, GraphRel, Join, JoinType, LogicalPlan},
        plan_ctx::PlanCtx,
        transformed::Transformed,
    },
};

pub struct GraphJoinInference;

impl AnalyzerPass for GraphJoinInference {
    fn analyze_with_graph_schema(
        &self,
        logical_plan: Arc<LogicalPlan>,
        plan_ctx: &mut PlanCtx,
        graph_schema: &GraphSchema,
    ) -> AnalyzerResult<Transformed<Arc<LogicalPlan>>> {
        let mut collected_graph_joins: Vec<Join> = vec![];
        let mut joined_entities: HashSet<String> = HashSet::new();
        self.collect_graph_joins(
            logical_plan.clone(),
            plan_ctx,
            graph_schema,
            &mut collected_graph_joins,
            &mut joined_entities,
        )?;
        if !collected_graph_joins.is_empty() {
            Self::build_graph_joins(logical_plan, &mut collected_graph_joins)
        } else {
            Ok(Transformed::No(logical_plan.clone()))
        }
    }
}

impl GraphJoinInference {
    pub fn new() -> Self {
        GraphJoinInference
    }

    fn build_graph_joins(
        logical_plan: Arc<LogicalPlan>,
        collected_graph_joins: &mut Vec<Join>,
    ) -> AnalyzerResult<Transformed<Arc<LogicalPlan>>> {
        let transformed_plan = match logical_plan.as_ref() {
            LogicalPlan::Projection(_) => {
                // wrap the outer projection i.e. first occurance in the tree walk with Graph joins
                Transformed::Yes(Arc::new(LogicalPlan::GraphJoins(GraphJoins {
                    input: logical_plan.clone(),
                    joins: collected_graph_joins.to_vec(),
                })))
            }
            LogicalPlan::GraphNode(graph_node) => {
                let child_tf =
                    Self::build_graph_joins(graph_node.input.clone(), collected_graph_joins)?;
                graph_node.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::GraphRel(graph_rel) => {
                let left_tf =
                    Self::build_graph_joins(graph_rel.left.clone(), collected_graph_joins)?;
                let center_tf =
                    Self::build_graph_joins(graph_rel.center.clone(), collected_graph_joins)?;
                let right_tf =
                    Self::build_graph_joins(graph_rel.right.clone(), collected_graph_joins)?;

                graph_rel.rebuild_or_clone(left_tf, center_tf, right_tf, logical_plan.clone())
            }
            LogicalPlan::Cte(cte) => {
                let child_tf = Self::build_graph_joins(cte.input.clone(), collected_graph_joins)?;
                cte.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Scan(_) => Transformed::No(logical_plan.clone()),
            LogicalPlan::Empty => Transformed::No(logical_plan.clone()),
            LogicalPlan::GraphJoins(graph_joins) => {
                let child_tf =
                    Self::build_graph_joins(graph_joins.input.clone(), collected_graph_joins)?;
                graph_joins.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Filter(filter) => {
                let child_tf =
                    Self::build_graph_joins(filter.input.clone(), collected_graph_joins)?;
                filter.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::GroupBy(group_by) => {
                let child_tf =
                    Self::build_graph_joins(group_by.input.clone(), collected_graph_joins)?;
                group_by.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::OrderBy(order_by) => {
                let child_tf =
                    Self::build_graph_joins(order_by.input.clone(), collected_graph_joins)?;
                order_by.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Skip(skip) => {
                let child_tf = Self::build_graph_joins(skip.input.clone(), collected_graph_joins)?;
                skip.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Limit(limit) => {
                let child_tf = Self::build_graph_joins(limit.input.clone(), collected_graph_joins)?;
                limit.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Union(union) => {
                let mut inputs_tf: Vec<Transformed<Arc<LogicalPlan>>> = vec![];
                for input_plan in union.inputs.iter() {
                    let child_tf =
                        Self::build_graph_joins(input_plan.clone(), collected_graph_joins)?;
                    inputs_tf.push(child_tf);
                }
                union.rebuild_or_clone(inputs_tf, logical_plan.clone())
            }
        };
        Ok(transformed_plan)
    }

    fn collect_graph_joins(
        &self,
        logical_plan: Arc<LogicalPlan>,
        plan_ctx: &mut PlanCtx,
        graph_schema: &GraphSchema,
        collected_graph_joins: &mut Vec<Join>,
        joined_entities: &mut HashSet<String>,
    ) -> AnalyzerResult<()> {
        match logical_plan.as_ref() {
            LogicalPlan::Projection(projection) => self.collect_graph_joins(
                projection.input.clone(),
                plan_ctx,
                graph_schema,
                collected_graph_joins,
                joined_entities,
            ),
            LogicalPlan::GraphNode(graph_node) => self.collect_graph_joins(
                graph_node.input.clone(),
                plan_ctx,
                graph_schema,
                collected_graph_joins,
                joined_entities,
            ),
            LogicalPlan::GraphRel(graph_rel) => {
                // infer joins for each graph_rel

                self.infer_graph_join(
                    graph_rel,
                    plan_ctx,
                    graph_schema,
                    collected_graph_joins,
                    joined_entities,
                )?;

                // self.collect_graph_joins(graph_rel.left.clone(), plan_ctx, graph_schema, collected_graph_joins, joined_entities);
                // self.collect_graph_joins(graph_rel.center.clone(), plan_ctx, graph_schema, collected_graph_joins, joined_entities);
                self.collect_graph_joins(
                    graph_rel.right.clone(),
                    plan_ctx,
                    graph_schema,
                    collected_graph_joins,
                    joined_entities,
                )
            }
            LogicalPlan::Cte(cte) => self.collect_graph_joins(
                cte.input.clone(),
                plan_ctx,
                graph_schema,
                collected_graph_joins,
                joined_entities,
            ),
            LogicalPlan::Scan(_) => Ok(()),
            LogicalPlan::Empty => Ok(()),
            LogicalPlan::GraphJoins(graph_joins) => self.collect_graph_joins(
                graph_joins.input.clone(),
                plan_ctx,
                graph_schema,
                collected_graph_joins,
                joined_entities,
            ),
            LogicalPlan::Filter(filter) => self.collect_graph_joins(
                filter.input.clone(),
                plan_ctx,
                graph_schema,
                collected_graph_joins,
                joined_entities,
            ),
            LogicalPlan::GroupBy(group_by) => self.collect_graph_joins(
                group_by.input.clone(),
                plan_ctx,
                graph_schema,
                collected_graph_joins,
                joined_entities,
            ),
            LogicalPlan::OrderBy(order_by) => self.collect_graph_joins(
                order_by.input.clone(),
                plan_ctx,
                graph_schema,
                collected_graph_joins,
                joined_entities,
            ),
            LogicalPlan::Skip(skip) => self.collect_graph_joins(
                skip.input.clone(),
                plan_ctx,
                graph_schema,
                collected_graph_joins,
                joined_entities,
            ),
            LogicalPlan::Limit(limit) => self.collect_graph_joins(
                limit.input.clone(),
                plan_ctx,
                graph_schema,
                collected_graph_joins,
                joined_entities,
            ),
            LogicalPlan::Union(union) => {
                for input_plan in union.inputs.iter() {
                    self.collect_graph_joins(
                        input_plan.clone(),
                        plan_ctx,
                        graph_schema,
                        collected_graph_joins,
                        joined_entities,
                    )?;
                }
                Ok(())
            }
        }
    }

    fn infer_graph_join(
        &self,
        graph_rel: &GraphRel,
        plan_ctx: &mut PlanCtx,
        graph_schema: &GraphSchema,
        collected_graph_joins: &mut Vec<Join>,
        joined_entities: &mut HashSet<String>,
    ) -> AnalyzerResult<()> {
        let graph_context = graph_context::get_graph_context(
            graph_rel,
            plan_ctx,
            graph_schema,
            Pass::GraphJoinInference,
        )?;

        // Check for standalone relationship join.
        // e.g. MATCH (a)-[f1:Follows]->(b)-[f2:Follows]->(c), (a)-[f3:Follows]->(c)
        // In the duplicate scan removing pass, we remove the already scanned nodes. We do this from bottom to up.
        // So there could be a graph_rel who has LogicalPlan::Empty as left. In such case just join the relationship but on both nodes columns.
        // In case of f3, both of its nodes a and b are already joined. So just join f3 on both a and b's joining keys.
        let is_standalone_rel: bool = matches!(graph_rel.left.as_ref(), LogicalPlan::Empty);

        let left_node_id_column = graph_context.left.schema.node_id.column.clone(); //  left_schema.node_id.column.clone();
        let right_node_id_column = graph_context.right.schema.node_id.column.clone(); //right_schema.node_id.column.clone();   

        if graph_context.rel.table_ctx.should_use_edge_list() {
            self.handle_edge_list_traversal(
                graph_rel,
                graph_context,
                left_node_id_column,
                right_node_id_column,
                is_standalone_rel,
                collected_graph_joins,
                joined_entities,
            )
        } else {
            self.handle_bitmap_traversal(
                graph_context,
                left_node_id_column,
                right_node_id_column,
                is_standalone_rel,
                collected_graph_joins,
                joined_entities,
            )
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn handle_edge_list_traversal(
        &self,
        graph_rel: &GraphRel,
        graph_context: GraphContext,
        left_node_id_column: String,
        right_node_id_column: String,
        is_standalone_rel: bool,
        collected_graph_joins: &mut Vec<Join>,
        joined_entities: &mut HashSet<String>,
    ) -> AnalyzerResult<()> {
        let left_alias = graph_context.left.alias;
        let rel_alias = graph_context.rel.alias;
        let right_alias = graph_context.right.alias;

        let left_cte_name = graph_context.left.cte_name;
        let rel_cte_name = graph_context.rel.cte_name;
        let right_cte_name = graph_context.right.cte_name;

        // If both nodes are of the same type then check the direction to determine where are the left and right nodes present in the edgelist.
        if graph_context.left.schema.table_name == graph_context.right.schema.table_name {
            if joined_entities.contains(right_alias) {
                // join the rel with right first and then join the left with rel
                let (rel_conn_with_right_node, left_conn_with_rel) =
                    if graph_rel.direction == Direction::Incoming {
                        ("from_id".to_string(), "to_id".to_string())
                    } else {
                        ("to_id".to_string(), "from_id".to_string())
                    };
                let mut rel_graph_join = Join {
                    table_name: rel_cte_name,
                    table_alias: rel_alias.to_string(),
                    joining_on: vec![OperatorApplication {
                        operator: Operator::Equal,
                        operands: vec![
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(rel_alias.to_string()),
                                column: Column(rel_conn_with_right_node),
                            }),
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(right_alias.to_string()),
                                column: Column(right_node_id_column.clone()),
                            }),
                        ],
                    }],
                    join_type: JoinType::Inner,
                };

                let left_graph_join = Join {
                    table_name: left_cte_name,
                    table_alias: left_alias.to_string(),
                    joining_on: vec![OperatorApplication {
                        operator: Operator::Equal,
                        operands: vec![
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(left_alias.to_string()),
                                column: Column(left_node_id_column.clone()),
                            }),
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(rel_alias.to_string()),
                                column: Column(left_conn_with_rel.clone()),
                            }),
                        ],
                    }],
                    join_type: JoinType::Inner,
                };

                if is_standalone_rel {
                    let rel_to_right_graph_join_keys = OperatorApplication {
                        operator: Operator::Equal,
                        operands: vec![
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(rel_alias.to_string()),
                                column: Column(left_conn_with_rel),
                            }),
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(left_alias.to_string()),
                                column: Column(left_node_id_column),
                            }),
                        ],
                    };
                    rel_graph_join.joining_on.push(rel_to_right_graph_join_keys);

                    collected_graph_joins.push(rel_graph_join);
                    joined_entities.insert(rel_alias.to_string());
                    // in this case we will only join relation so early return without pushing the other joins
                    return Ok(());
                }

                // push the relation first
                collected_graph_joins.push(rel_graph_join);
                joined_entities.insert(rel_alias.to_string());

                joined_entities.insert(left_alias.to_string());
                collected_graph_joins.push(left_graph_join);
                Ok(())
            } else {
                // When left is already joined or start of the join

                // join the relation with left side first and then
                // the join the right side with relation

                let (rel_conn_with_left_node, right_conn_with_rel) =
                    if graph_rel.direction == Direction::Incoming {
                        ("from_id".to_string(), "to_id".to_string())
                    } else {
                        ("to_id".to_string(), "from_id".to_string())
                    };

                let mut rel_graph_join = Join {
                    table_name: rel_cte_name,
                    table_alias: rel_alias.to_string(),
                    joining_on: vec![OperatorApplication {
                        operator: Operator::Equal,
                        operands: vec![
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(rel_alias.to_string()),
                                column: Column(rel_conn_with_left_node),
                            }),
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(left_alias.to_string()),
                                column: Column(left_node_id_column.clone()),
                            }),
                        ],
                    }],
                    join_type: JoinType::Inner,
                };

                let right_graph_join = Join {
                    table_name: right_cte_name,
                    table_alias: right_alias.to_string(),
                    joining_on: vec![OperatorApplication {
                        operator: Operator::Equal,
                        operands: vec![
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(right_alias.to_string()),
                                column: Column(right_node_id_column.clone()),
                            }),
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(rel_alias.to_string()),
                                column: Column(right_conn_with_rel.clone()),
                            }),
                        ],
                    }],
                    join_type: JoinType::Inner,
                };

                if is_standalone_rel {
                    let rel_to_right_graph_join_keys = OperatorApplication {
                        operator: Operator::Equal,
                        operands: vec![
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(rel_alias.to_string()),
                                column: Column(right_conn_with_rel),
                            }),
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(right_alias.to_string()),
                                column: Column(right_node_id_column),
                            }),
                        ],
                    };
                    rel_graph_join.joining_on.push(rel_to_right_graph_join_keys);

                    collected_graph_joins.push(rel_graph_join);
                    joined_entities.insert(rel_alias.to_string());
                    // in this case we will only join relation so early return without pushing the other joins
                    return Ok(());
                }

                // push the relation first
                collected_graph_joins.push(rel_graph_join);
                joined_entities.insert(rel_alias.to_string());

                joined_entities.insert(right_alias.to_string());
                collected_graph_joins.push(right_graph_join);
                Ok(())
            }
        } else
        // check if right is connected with edge list's from_node
        if graph_context.rel.schema.from_node == graph_context.right.schema.table_name {
            // this means rel.from_node = right and to_node = left

            // check if right is already joined
            if joined_entities.contains(right_alias) {
                // join the rel with right first and then join the left with rel
                let mut rel_graph_join = Join {
                    table_name: rel_cte_name,
                    table_alias: rel_alias.to_string(),
                    joining_on: vec![OperatorApplication {
                        operator: Operator::Equal,
                        operands: vec![
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(rel_alias.to_string()),
                                column: Column("from_id".to_string()),
                            }),
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(right_alias.to_string()),
                                column: Column(right_node_id_column.clone()),
                            }),
                        ],
                    }],
                    join_type: JoinType::Inner,
                };

                let left_graph_join = Join {
                    table_name: left_cte_name,
                    table_alias: left_alias.to_string(),
                    joining_on: vec![OperatorApplication {
                        operator: Operator::Equal,
                        operands: vec![
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(left_alias.to_string()),
                                column: Column(left_node_id_column.clone()),
                            }),
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(rel_alias.to_string()),
                                column: Column("to_id".to_string()),
                            }),
                        ],
                    }],
                    join_type: JoinType::Inner,
                };

                if is_standalone_rel {
                    let rel_to_right_graph_join_keys = OperatorApplication {
                        operator: Operator::Equal,
                        operands: vec![
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(rel_alias.to_string()),
                                column: Column("to_id".to_string()),
                            }),
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(left_alias.to_string()),
                                column: Column(left_node_id_column),
                            }),
                        ],
                    };
                    rel_graph_join.joining_on.push(rel_to_right_graph_join_keys);

                    collected_graph_joins.push(rel_graph_join);
                    joined_entities.insert(rel_alias.to_string());
                    // in this case we will only join relation so early return without pushing the other joins
                    return Ok(());
                }

                // push the relation first
                collected_graph_joins.push(rel_graph_join);
                joined_entities.insert(rel_alias.to_string());

                joined_entities.insert(left_alias.to_string());
                collected_graph_joins.push(left_graph_join);
                Ok(())
            } else {
                // When left is already joined or start of the join

                // join the relation with left side first and then
                // the join the right side with relation
                let mut rel_graph_join = Join {
                    table_name: rel_cte_name,
                    table_alias: rel_alias.to_string(),
                    joining_on: vec![OperatorApplication {
                        operator: Operator::Equal,
                        operands: vec![
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(rel_alias.to_string()),
                                column: Column("to_id".to_string()),
                            }),
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(left_alias.to_string()),
                                column: Column(left_node_id_column.clone()),
                            }),
                        ],
                    }],
                    join_type: JoinType::Inner,
                };

                let right_graph_join = Join {
                    table_name: right_cte_name,
                    table_alias: right_alias.to_string(),
                    joining_on: vec![OperatorApplication {
                        operator: Operator::Equal,
                        operands: vec![
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(right_alias.to_string()),
                                column: Column(right_node_id_column.clone()),
                            }),
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(rel_alias.to_string()),
                                column: Column("from_id".to_string()),
                            }),
                        ],
                    }],
                    join_type: JoinType::Inner,
                };

                if is_standalone_rel {
                    let rel_to_right_graph_join_keys = OperatorApplication {
                        operator: Operator::Equal,
                        operands: vec![
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(rel_alias.to_string()),
                                column: Column("from_id".to_string()),
                            }),
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(right_alias.to_string()),
                                column: Column(right_node_id_column),
                            }),
                        ],
                    };
                    rel_graph_join.joining_on.push(rel_to_right_graph_join_keys);

                    collected_graph_joins.push(rel_graph_join);
                    joined_entities.insert(rel_alias.to_string());
                    // in this case we will only join relation so early return without pushing the other joins
                    return Ok(());
                }

                // push the relation first
                collected_graph_joins.push(rel_graph_join);
                joined_entities.insert(rel_alias.to_string());

                joined_entities.insert(right_alias.to_string());
                collected_graph_joins.push(right_graph_join);
                Ok(())
            }
        } else {
            // this means rel.from_node = left and to_node = right

            // check if right is already joined
            if joined_entities.contains(right_alias) {
                // join the rel with right first and then join the left with rel
                let mut rel_graph_join = Join {
                    table_name: rel_cte_name,
                    table_alias: rel_alias.to_string(),
                    joining_on: vec![OperatorApplication {
                        operator: Operator::Equal,
                        operands: vec![
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(rel_alias.to_string()),
                                column: Column("to_id".to_string()),
                            }),
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(right_alias.to_string()),
                                column: Column(right_node_id_column.clone()),
                            }),
                        ],
                    }],
                    join_type: JoinType::Inner,
                };

                let left_graph_join = Join {
                    table_name: left_cte_name,
                    table_alias: left_alias.to_string(),
                    joining_on: vec![OperatorApplication {
                        operator: Operator::Equal,
                        operands: vec![
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(left_alias.to_string()),
                                column: Column(left_node_id_column.clone()),
                            }),
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(rel_alias.to_string()),
                                column: Column("from_id".to_string()),
                            }),
                        ],
                    }],
                    join_type: JoinType::Inner,
                };

                if is_standalone_rel {
                    let rel_to_right_graph_join_keys = OperatorApplication {
                        operator: Operator::Equal,
                        operands: vec![
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(rel_alias.to_string()),
                                column: Column("from_id".to_string()),
                            }),
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(left_alias.to_string()),
                                column: Column(left_node_id_column),
                            }),
                        ],
                    };
                    rel_graph_join.joining_on.push(rel_to_right_graph_join_keys);

                    collected_graph_joins.push(rel_graph_join);
                    joined_entities.insert(rel_alias.to_string());
                    // in this case we will only join relation so early return without pushing the other joins
                    return Ok(());
                }

                // push the relation first
                collected_graph_joins.push(rel_graph_join);
                joined_entities.insert(rel_alias.to_string());

                joined_entities.insert(left_alias.to_string());
                collected_graph_joins.push(left_graph_join);
                Ok(())
            } else {
                // When left is already joined or start of the join

                // join the relation with left side first and then
                // the join the right side with relation
                let mut rel_graph_join = Join {
                    table_name: rel_cte_name,
                    table_alias: rel_alias.to_string(),
                    joining_on: vec![OperatorApplication {
                        operator: Operator::Equal,
                        operands: vec![
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(rel_alias.to_string()),
                                column: Column("from_id".to_string()),
                            }),
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(left_alias.to_string()),
                                column: Column(left_node_id_column.clone()),
                            }),
                        ],
                    }],
                    join_type: JoinType::Inner,
                };

                let right_graph_join = Join {
                    table_name: right_cte_name,
                    table_alias: right_alias.to_string(),
                    joining_on: vec![OperatorApplication {
                        operator: Operator::Equal,
                        operands: vec![
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(right_alias.to_string()),
                                column: Column(right_node_id_column.clone()),
                            }),
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(rel_alias.to_string()),
                                column: Column("to_id".to_string()),
                            }),
                        ],
                    }],
                    join_type: JoinType::Inner,
                };

                if is_standalone_rel {
                    let rel_to_right_graph_join_keys = OperatorApplication {
                        operator: Operator::Equal,
                        operands: vec![
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(rel_alias.to_string()),
                                column: Column("to_id".to_string()),
                            }),
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(right_alias.to_string()),
                                column: Column(right_node_id_column),
                            }),
                        ],
                    };
                    rel_graph_join.joining_on.push(rel_to_right_graph_join_keys);

                    collected_graph_joins.push(rel_graph_join);
                    joined_entities.insert(rel_alias.to_string());
                    // in this case we will only join relation so early return without pushing the other joins
                    return Ok(());
                }

                // push the relation first
                collected_graph_joins.push(rel_graph_join);
                joined_entities.insert(rel_alias.to_string());

                joined_entities.insert(right_alias.to_string());
                collected_graph_joins.push(right_graph_join);
                Ok(())
            }
        }
    }

    fn handle_bitmap_traversal(
        &self,
        graph_context: GraphContext,
        left_node_id_column: String,
        right_node_id_column: String,
        is_standalone_rel: bool,
        collected_graph_joins: &mut Vec<Join>,
        joined_entities: &mut HashSet<String>,
    ) -> AnalyzerResult<()> {
        let left_alias = graph_context.left.alias;
        let rel_alias = graph_context.rel.alias;
        let right_alias = graph_context.right.alias;

        let left_cte_name = graph_context.left.cte_name;
        // let rel_cte_name = format!("{}_{}", rel_label, rel_alias);
        let rel_cte_name = graph_context.rel.cte_name;
        let right_cte_name = graph_context.right.cte_name;

        // check if right is alredy joined.
        if joined_entities.contains(right_alias) {
            // join the rel with right first and then join the left with rel
            let mut rel_graph_join = Join {
                table_name: rel_cte_name,
                table_alias: rel_alias.to_string(),
                joining_on: vec![OperatorApplication {
                    operator: Operator::Equal,
                    operands: vec![
                        LogicalExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: TableAlias(rel_alias.to_string()),
                            column: Column("from_id".to_string()),
                        }),
                        LogicalExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: TableAlias(right_alias.to_string()),
                            column: Column(right_node_id_column.clone()),
                        }),
                    ],
                }],
                join_type: JoinType::Inner,
            };

            let left_graph_join = Join {
                table_name: left_cte_name,
                table_alias: left_alias.to_string(),
                joining_on: vec![OperatorApplication {
                    operator: Operator::Equal,
                    operands: vec![
                        LogicalExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: TableAlias(left_alias.to_string()),
                            column: Column(left_node_id_column.clone()),
                        }),
                        LogicalExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: TableAlias(rel_alias.to_string()),
                            column: Column("to_id".to_string()),
                        }),
                    ],
                }],
                join_type: JoinType::Inner,
            };

            if is_standalone_rel {
                let rel_to_right_graph_join_keys = OperatorApplication {
                    operator: Operator::Equal,
                    operands: vec![
                        LogicalExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: TableAlias(rel_alias.to_string()),
                            column: Column("to_id".to_string()),
                        }),
                        LogicalExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: TableAlias(left_alias.to_string()),
                            column: Column(left_node_id_column),
                        }),
                    ],
                };
                rel_graph_join.joining_on.push(rel_to_right_graph_join_keys);

                collected_graph_joins.push(rel_graph_join);
                joined_entities.insert(rel_alias.to_string());
                // in this case we will only join relation so early return without pushing the other joins
                return Ok(());
            }

            // push the relation first
            collected_graph_joins.push(rel_graph_join);
            joined_entities.insert(rel_alias.to_string());

            joined_entities.insert(left_alias.to_string());
            collected_graph_joins.push(left_graph_join);
            Ok(())
        } else {
            // When left is already joined or start of the join

            // join the relation with left side first and then
            // the join the right side with relation
            let mut rel_graph_join = Join {
                table_name: rel_cte_name,
                table_alias: rel_alias.to_string(),
                joining_on: vec![OperatorApplication {
                    operator: Operator::Equal,
                    operands: vec![
                        LogicalExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: TableAlias(rel_alias.to_string()),
                            column: Column("to_id".to_string()),
                        }),
                        LogicalExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: TableAlias(left_alias.to_string()),
                            column: Column(left_node_id_column.clone()),
                        }),
                    ],
                }],
                join_type: JoinType::Inner,
            };

            let right_graph_join = Join {
                table_name: right_cte_name,
                table_alias: right_alias.to_string(),
                joining_on: vec![OperatorApplication {
                    operator: Operator::Equal,
                    operands: vec![
                        LogicalExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: TableAlias(right_alias.to_string()),
                            column: Column(right_node_id_column.clone()),
                        }),
                        LogicalExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: TableAlias(rel_alias.to_string()),
                            column: Column("from_id".to_string()),
                        }),
                    ],
                }],
                join_type: JoinType::Inner,
            };

            if is_standalone_rel {
                let rel_to_right_graph_join_keys = OperatorApplication {
                    operator: Operator::Equal,
                    operands: vec![
                        LogicalExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: TableAlias(rel_alias.to_string()),
                            column: Column("from_id".to_string()),
                        }),
                        LogicalExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: TableAlias(right_alias.to_string()),
                            column: Column(right_node_id_column),
                        }),
                    ],
                };
                rel_graph_join.joining_on.push(rel_to_right_graph_join_keys);

                collected_graph_joins.push(rel_graph_join);
                joined_entities.insert(rel_alias.to_string());
                // in this case we will only join relation so early return without pushing the other joins
                return Ok(());
            }

            // push the relation first
            collected_graph_joins.push(rel_graph_join);
            joined_entities.insert(rel_alias.to_string());

            joined_entities.insert(right_alias.to_string());
            collected_graph_joins.push(right_graph_join);
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        graph_catalog::graph_schema::{GraphSchema, NodeIdSchema, NodeSchema, RelationshipSchema},
        query_planner::{
            logical_expr::{Column, Direction, LogicalExpr, Operator, PropertyAccess, TableAlias},
            logical_plan::{
                GraphNode, GraphRel, JoinType, LogicalPlan, Projection, ProjectionItem, Scan,
            },
            plan_ctx::{PlanCtx, TableCtx},
        },
    };
    use std::collections::HashMap;

    fn create_test_graph_schema() -> GraphSchema {
        let mut nodes = HashMap::new();
        let mut relationships = HashMap::new();

        // Create Person node schema
        nodes.insert(
            "Person".to_string(),
            NodeSchema {
                table_name: "Person".to_string(),
                column_names: vec!["id".to_string(), "name".to_string(), "age".to_string()],
                primary_keys: "id".to_string(),
                node_id: NodeIdSchema {
                    column: "id".to_string(),
                    dtype: "UInt64".to_string(),
                },
            },
        );

        // Create Company node schema
        nodes.insert(
            "Company".to_string(),
            NodeSchema {
                table_name: "Company".to_string(),
                column_names: vec!["id".to_string(), "name".to_string(), "founded".to_string()],
                primary_keys: "id".to_string(),
                node_id: NodeIdSchema {
                    column: "id".to_string(),
                    dtype: "UInt64".to_string(),
                },
            },
        );

        // Create FOLLOWS relationship schema (edge list)
        relationships.insert(
            "FOLLOWS".to_string(),
            RelationshipSchema {
                table_name: "FOLLOWS".to_string(),
                column_names: vec![
                    "from_id".to_string(),
                    "to_id".to_string(),
                    "since".to_string(),
                ],
                from_node: "Person".to_string(),
                to_node: "Person".to_string(),
                from_node_id_dtype: "UInt64".to_string(),
                to_node_id_dtype: "UInt64".to_string(),
            },
        );

        // Create WORKS_AT relationship schema (edge list)
        relationships.insert(
            "WORKS_AT".to_string(),
            RelationshipSchema {
                table_name: "WORKS_AT".to_string(),
                column_names: vec![
                    "from_id".to_string(),
                    "to_id".to_string(),
                    "position".to_string(),
                ],
                from_node: "Person".to_string(),
                to_node: "Company".to_string(),
                from_node_id_dtype: "UInt64".to_string(),
                to_node_id_dtype: "UInt64".to_string(),
            },
        );

        GraphSchema::build(1, nodes, relationships, HashMap::new())
    }

    fn setup_plan_ctx_with_graph_entities() -> PlanCtx {
        let mut plan_ctx = PlanCtx::default();

        // Add person nodes
        plan_ctx.insert_table_ctx(
            "p1".to_string(),
            TableCtx::build(
                "p1".to_string(),
                Some("Person".to_string()),
                vec![],
                false,
                true,
            ),
        );
        plan_ctx.insert_table_ctx(
            "p2".to_string(),
            TableCtx::build(
                "p2".to_string(),
                Some("Person".to_string()),
                vec![],
                false,
                true,
            ),
        );
        plan_ctx.insert_table_ctx(
            "p3".to_string(),
            TableCtx::build(
                "p3".to_string(),
                Some("Person".to_string()),
                vec![],
                false,
                true,
            ),
        );

        // Add company node
        plan_ctx.insert_table_ctx(
            "c1".to_string(),
            TableCtx::build(
                "c1".to_string(),
                Some("Company".to_string()),
                vec![],
                false,
                true,
            ),
        );

        // Add follows relationships
        plan_ctx.insert_table_ctx(
            "f1".to_string(),
            TableCtx::build(
                "f1".to_string(),
                Some("FOLLOWS".to_string()),
                vec![],
                true,
                true,
            ),
        );
        plan_ctx.insert_table_ctx(
            "f2".to_string(),
            TableCtx::build(
                "f2".to_string(),
                Some("FOLLOWS".to_string()),
                vec![],
                true,
                true,
            ),
        );

        // Add works_at relationship
        plan_ctx.insert_table_ctx(
            "w1".to_string(),
            TableCtx::build(
                "w1".to_string(),
                Some("WORKS_AT".to_string()),
                vec![],
                true,
                true,
            ),
        );

        plan_ctx
    }

    fn create_scan_plan(table_alias: &str, table_name: &str) -> Arc<LogicalPlan> {
        Arc::new(LogicalPlan::Scan(Scan {
            table_alias: Some(table_alias.to_string()),
            table_name: Some(table_name.to_string()),
        }))
    }

    fn create_graph_node(input: Arc<LogicalPlan>, alias: &str) -> Arc<LogicalPlan> {
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
        direction: Direction,
        left_connection: &str,
        right_connection: &str,
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
    fn test_no_graph_joins_when_no_graph_rels() {
        let analyzer = GraphJoinInference::new();
        let graph_schema = create_test_graph_schema();
        let mut plan_ctx = setup_plan_ctx_with_graph_entities();

        // Create a plan with only a graph node (no relationships)
        let scan = create_scan_plan("p1", "person");
        let graph_node = create_graph_node(scan, "p1");

        let result = analyzer
            .analyze_with_graph_schema(graph_node.clone(), &mut plan_ctx, &graph_schema)
            .unwrap();

        // Should not transform the plan since there are no graph relationships
        match result {
            Transformed::No(plan) => {
                assert_eq!(plan, graph_node);
            }
            _ => panic!("Expected no transformation for plan without relationships"),
        }
    }

    #[test]
    fn test_edge_list_same_node_type_outgoing_direction() {
        let analyzer = GraphJoinInference::new();
        let graph_schema = create_test_graph_schema();
        let mut plan_ctx = setup_plan_ctx_with_graph_entities();

        // Set the relationship to use edge list
        plan_ctx
            .get_mut_table_ctx("f1")
            .unwrap()
            .set_use_edge_list(true);

        // Create plan: (p1)-[f1:FOLLOWS]->(p2)
        let p1_scan = create_scan_plan("p1", "Person");
        let p1_node = create_graph_node(p1_scan, "p1");

        let f1_scan = create_scan_plan("f1", "FOLLOWS");

        let p2_scan = create_scan_plan("p2", "Person");
        let p2_node = create_graph_node(p2_scan, "p2");

        let graph_rel = create_graph_rel(
            p2_node,
            f1_scan,
            p1_node,
            "f1",
            Direction::Outgoing,
            "p2",
            "p1",
        );

        let input_logical_plan = Arc::new(LogicalPlan::Projection(Projection {
            input: graph_rel,
            items: vec![ProjectionItem {
                expression: LogicalExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias("p1".to_string()),
                    column: Column("name".to_string()),
                }),
                col_alias: None,
            }],
        }));

        let result = analyzer
            .analyze_with_graph_schema(input_logical_plan, &mut plan_ctx, &graph_schema)
            .unwrap();

        println!("\n result: {:?}\n", result);

        // Should create graph joins
        match result {
            Transformed::Yes(plan) => {
                match plan.as_ref() {
                    LogicalPlan::GraphJoins(graph_joins) => {
                        // Assert GraphJoins structure
                        assert_eq!(graph_joins.joins.len(), 2);
                        assert!(matches!(
                            graph_joins.input.as_ref(),
                            LogicalPlan::Projection(_)
                        ));

                        // First join should be relationship with left node
                        let rel_join = &graph_joins.joins[0];
                        assert_eq!(rel_join.table_name, "FOLLOWS_f1");
                        assert_eq!(rel_join.table_alias, "f1");
                        assert_eq!(rel_join.join_type, JoinType::Inner);
                        assert_eq!(rel_join.joining_on.len(), 1);

                        // Assert the joining condition for relationship
                        let rel_join_condition = &rel_join.joining_on[0];
                        assert_eq!(rel_join_condition.operator, Operator::Equal);
                        assert_eq!(rel_join_condition.operands.len(), 2);

                        // Check operands are PropertyAccessExp with correct table aliases and columns
                        match (
                            &rel_join_condition.operands[0],
                            &rel_join_condition.operands[1],
                        ) {
                            (
                                LogicalExpr::PropertyAccessExp(rel_prop),
                                LogicalExpr::PropertyAccessExp(left_prop),
                            ) => {
                                assert_eq!(rel_prop.table_alias.0, "f1");
                                assert_eq!(rel_prop.column.0, "to_id");
                                assert_eq!(left_prop.table_alias.0, "p2");
                                assert_eq!(left_prop.column.0, "id");
                            }
                            _ => panic!("Expected PropertyAccessExp operands"),
                        }

                        // Second join should be right node with relationship
                        let right_join = &graph_joins.joins[1];
                        assert_eq!(right_join.table_name, "Person_p1");
                        assert_eq!(right_join.table_alias, "p1");
                        assert_eq!(right_join.join_type, JoinType::Inner);
                        assert_eq!(right_join.joining_on.len(), 1);

                        // Assert the joining condition for right node
                        let right_join_condition = &right_join.joining_on[0];
                        assert_eq!(right_join_condition.operator, Operator::Equal);
                        assert_eq!(right_join_condition.operands.len(), 2);

                        // Check operands are PropertyAccessExp with correct table aliases and columns
                        match (
                            &right_join_condition.operands[0],
                            &right_join_condition.operands[1],
                        ) {
                            (
                                LogicalExpr::PropertyAccessExp(left_prop),
                                LogicalExpr::PropertyAccessExp(rel_prop),
                            ) => {
                                assert_eq!(left_prop.table_alias.0, "p1");
                                assert_eq!(left_prop.column.0, "id");
                                assert_eq!(rel_prop.table_alias.0, "f1");
                                assert_eq!(rel_prop.column.0, "from_id");
                            }
                            _ => panic!("Expected PropertyAccessExp operands"),
                        }
                    }
                    _ => panic!("Expected GraphJoins node"),
                }
            }
            _ => panic!("Expected transformation"),
        }
    }

    #[test]
    fn test_edge_list_different_node_types() {
        let analyzer = GraphJoinInference::new();
        let graph_schema = create_test_graph_schema();
        let mut plan_ctx = setup_plan_ctx_with_graph_entities();

        // Set the relationship to use edge list
        plan_ctx
            .get_mut_table_ctx("w1")
            .unwrap()
            .set_use_edge_list(true);

        // Create plan: (p1)-[w1:WORKS_AT]->(c1)
        let p1_scan = create_scan_plan("p1", "Person");
        let p1_node = create_graph_node(p1_scan, "p1");

        let w1_scan = create_scan_plan("w1", "WORKS_AT");

        let c1_scan = create_scan_plan("c1", "Company");
        let c1_node = create_graph_node(c1_scan, "c1");

        let graph_rel = create_graph_rel(
            p1_node,
            w1_scan,
            c1_node,
            "w1",
            Direction::Outgoing,
            "c1",
            "p1",
        );

        let input_logical_plan = Arc::new(LogicalPlan::Projection(Projection {
            input: graph_rel,
            items: vec![ProjectionItem {
                expression: LogicalExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias("p1".to_string()),
                    column: Column("name".to_string()),
                }),
                col_alias: None,
            }],
        }));

        let result = analyzer
            .analyze_with_graph_schema(input_logical_plan, &mut plan_ctx, &graph_schema)
            .unwrap();

        // Should create graph joins for different node types
        match result {
            Transformed::Yes(plan) => {
                match plan.as_ref() {
                    LogicalPlan::GraphJoins(graph_joins) => {
                        // Assert GraphJoins structure
                        assert_eq!(graph_joins.joins.len(), 2);
                        assert!(matches!(
                            graph_joins.input.as_ref(),
                            LogicalPlan::Projection(_)
                        ));
                        // (p1)-[w1:WORKS_AT]->(c1)
                        // First join should be relationship with left node (from_id)
                        let rel_join = &graph_joins.joins[0];
                        assert_eq!(rel_join.table_name, "WORKS_AT_w1");
                        assert_eq!(rel_join.table_alias, "w1");
                        assert_eq!(rel_join.join_type, JoinType::Inner);
                        assert_eq!(rel_join.joining_on.len(), 1);

                        // Assert the joining condition for relationship
                        let rel_join_condition = &rel_join.joining_on[0];
                        assert_eq!(rel_join_condition.operator, Operator::Equal);
                        assert_eq!(rel_join_condition.operands.len(), 2);

                        // Check operands are PropertyAccessExp with correct table aliases and columns
                        match (
                            &rel_join_condition.operands[0],
                            &rel_join_condition.operands[1],
                        ) {
                            (
                                LogicalExpr::PropertyAccessExp(rel_prop),
                                LogicalExpr::PropertyAccessExp(left_prop),
                            ) => {
                                assert_eq!(rel_prop.table_alias.0, "w1");
                                assert_eq!(rel_prop.column.0, "to_id");
                                assert_eq!(left_prop.table_alias.0, "c1");
                                assert_eq!(left_prop.column.0, "id");
                            }
                            _ => panic!("Expected PropertyAccessExp operands"),
                        }

                        // Second join should be right node with relationship (from_id)
                        let right_join = &graph_joins.joins[1];
                        assert_eq!(right_join.table_name, "Person_p1");
                        assert_eq!(right_join.table_alias, "p1");
                        assert_eq!(right_join.join_type, JoinType::Inner);
                        assert_eq!(right_join.joining_on.len(), 1);

                        // Assert the joining condition for right node
                        let right_join_condition = &right_join.joining_on[0];
                        assert_eq!(right_join_condition.operator, Operator::Equal);
                        assert_eq!(right_join_condition.operands.len(), 2);

                        // Check operands are PropertyAccessExp with correct table aliases and columns
                        match (
                            &right_join_condition.operands[0],
                            &right_join_condition.operands[1],
                        ) {
                            (
                                LogicalExpr::PropertyAccessExp(right_prop),
                                LogicalExpr::PropertyAccessExp(rel_prop),
                            ) => {
                                assert_eq!(right_prop.table_alias.0, "p1");
                                assert_eq!(right_prop.column.0, "id");
                                assert_eq!(rel_prop.table_alias.0, "w1");
                                assert_eq!(rel_prop.column.0, "from_id");
                            }
                            _ => panic!("Expected PropertyAccessExp operands"),
                        }
                    }
                    _ => panic!("Expected GraphJoins node"),
                }
            }
            _ => panic!("Expected transformation"),
        }
    }

    #[test]
    fn test_bitmap_traversal() {
        let analyzer = GraphJoinInference::new();
        let graph_schema = create_test_graph_schema();
        let mut plan_ctx = setup_plan_ctx_with_graph_entities();

        // Don't set use_edge_list, so it will use bitmap traversal
        assert!(!plan_ctx.get_table_ctx("f1").unwrap().should_use_edge_list());

        // Create plan: (p1)-[f1:FOLLOWS]->(p2)
        let p1_scan = create_scan_plan("p1", "Person");
        let p1_node = create_graph_node(p1_scan, "p1");

        let f1_scan = create_scan_plan("f1", "FOLLOWS");

        // Add follows relationships
        plan_ctx.insert_table_ctx(
            "f1".to_string(),
            TableCtx::build(
                "f1".to_string(),
                Some("FOLLOWS_outgoing".to_string()),
                vec![],
                true,
                true,
            ),
        );

        let p2_scan = create_scan_plan("p2", "Person");
        let p2_node = create_graph_node(p2_scan, "p2");

        let graph_rel = create_graph_rel(
            p2_node,
            f1_scan,
            p1_node,
            "f1",
            Direction::Outgoing,
            "p2",
            "p1",
        );

        let input_logical_plan = Arc::new(LogicalPlan::Projection(Projection {
            input: graph_rel,
            items: vec![ProjectionItem {
                expression: LogicalExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias("p1".to_string()),
                    column: Column("name".to_string()),
                }),
                col_alias: None,
            }],
        }));

        let result = analyzer
            .analyze_with_graph_schema(input_logical_plan, &mut plan_ctx, &graph_schema)
            .unwrap();

        // Should create graph joins for bitmap traversal
        match result {
            Transformed::Yes(plan) => {
                match plan.as_ref() {
                    LogicalPlan::GraphJoins(graph_joins) => {
                        // Assert GraphJoins structure
                        assert_eq!(graph_joins.joins.len(), 2);
                        assert!(matches!(
                            graph_joins.input.as_ref(),
                            LogicalPlan::Projection(_)
                        ));

                        // (p1)-[f1:FOLLOWS]->(p2)
                        // For bitmap traversal, joins are different from edge list
                        let rel_join = &graph_joins.joins[0];
                        assert_eq!(rel_join.table_name, "FOLLOWS_outgoing_f1");
                        assert_eq!(rel_join.table_alias, "f1");
                        assert_eq!(rel_join.join_type, JoinType::Inner);
                        assert_eq!(rel_join.joining_on.len(), 1);

                        // Assert the joining condition for relationship
                        let rel_join_condition = &rel_join.joining_on[0];
                        assert_eq!(rel_join_condition.operator, Operator::Equal);
                        assert_eq!(rel_join_condition.operands.len(), 2);

                        // Check operands are PropertyAccessExp with correct table aliases and columns
                        match (
                            &rel_join_condition.operands[0],
                            &rel_join_condition.operands[1],
                        ) {
                            (
                                LogicalExpr::PropertyAccessExp(rel_prop),
                                LogicalExpr::PropertyAccessExp(left_prop),
                            ) => {
                                assert_eq!(rel_prop.table_alias.0, "f1");
                                assert_eq!(rel_prop.column.0, "to_id");
                                assert_eq!(left_prop.table_alias.0, "p2");
                                assert_eq!(left_prop.column.0, "id");
                            }
                            _ => panic!("Expected PropertyAccessExp operands"),
                        }

                        let right_join = &graph_joins.joins[1];
                        assert_eq!(right_join.table_name, "Person_p1");
                        assert_eq!(right_join.table_alias, "p1");
                        assert_eq!(right_join.join_type, JoinType::Inner);
                        assert_eq!(right_join.joining_on.len(), 1);

                        // Assert the joining condition for right node
                        let right_join_condition = &right_join.joining_on[0];
                        assert_eq!(right_join_condition.operator, Operator::Equal);
                        assert_eq!(right_join_condition.operands.len(), 2);

                        // Check operands are PropertyAccessExp with correct table aliases and columns
                        match (
                            &right_join_condition.operands[0],
                            &right_join_condition.operands[1],
                        ) {
                            (
                                LogicalExpr::PropertyAccessExp(right_prop),
                                LogicalExpr::PropertyAccessExp(rel_prop),
                            ) => {
                                assert_eq!(right_prop.table_alias.0, "p1");
                                assert_eq!(right_prop.column.0, "id");
                                assert_eq!(rel_prop.table_alias.0, "f1");
                                assert_eq!(rel_prop.column.0, "from_id");
                            }
                            _ => panic!("Expected PropertyAccessExp operands"),
                        }
                    }
                    _ => panic!("Expected GraphJoins node"),
                }
            }
            _ => panic!("Expected transformation"),
        }
    }

    #[test]
    fn test_standalone_relationship_edge_list() {
        let analyzer = GraphJoinInference::new();
        let graph_schema = create_test_graph_schema();
        let mut plan_ctx = setup_plan_ctx_with_graph_entities();

        // Set the relationship to use edge list
        plan_ctx
            .get_mut_table_ctx("f2")
            .unwrap()
            .set_use_edge_list(true);

        // Create standalone relationship: (p3)-[f2:FOLLOWS]-(Empty)
        // This simulates a case where left node was already processed/removed
        let empty_left = Arc::new(LogicalPlan::Empty);
        let f2_scan = create_scan_plan("f2", "FOLLOWS");
        let p3_scan = create_scan_plan("p3", "Person");
        let p3_node = create_graph_node(p3_scan, "p3");

        let graph_rel = create_graph_rel(
            empty_left,
            f2_scan,
            p3_node,
            "f2",
            Direction::Outgoing,
            "p1", // left connection exists but left plan is Empty
            "p3",
        );

        let input_logical_plan = Arc::new(LogicalPlan::Projection(Projection {
            input: graph_rel,
            items: vec![ProjectionItem {
                expression: LogicalExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias("p1".to_string()),
                    column: Column("name".to_string()),
                }),
                col_alias: None,
            }],
        }));

        let result = analyzer
            .analyze_with_graph_schema(input_logical_plan, &mut plan_ctx, &graph_schema)
            .unwrap();

        // Should create only relationship join with both node connections
        match result {
            Transformed::Yes(plan) => {
                match plan.as_ref() {
                    LogicalPlan::GraphJoins(graph_joins) => {
                        // Assert GraphJoins structure
                        assert_eq!(graph_joins.joins.len(), 1); // Only relationship join
                        assert!(matches!(
                            graph_joins.input.as_ref(),
                            LogicalPlan::Projection(_)
                        ));

                        let rel_join = &graph_joins.joins[0];
                        assert_eq!(rel_join.table_name, "FOLLOWS_f2");
                        assert_eq!(rel_join.table_alias, "f2");
                        assert_eq!(rel_join.join_type, JoinType::Inner);
                        // Should have 2 join conditions for standalone rel
                        assert_eq!(rel_join.joining_on.len(), 2);

                        // Assert the first joining condition (connection to right node)
                        let first_join_condition = &rel_join.joining_on[0];
                        assert_eq!(first_join_condition.operator, Operator::Equal);
                        assert_eq!(first_join_condition.operands.len(), 2);

                        match (
                            &first_join_condition.operands[0],
                            &first_join_condition.operands[1],
                        ) {
                            (
                                LogicalExpr::PropertyAccessExp(rel_prop),
                                LogicalExpr::PropertyAccessExp(left_prop),
                            ) => {
                                assert_eq!(rel_prop.table_alias.0, "f2");
                                assert_eq!(rel_prop.column.0, "to_id");
                                assert_eq!(left_prop.table_alias.0, "p1");
                                assert_eq!(left_prop.column.0, "id");
                            }
                            _ => panic!("Expected PropertyAccessExp operands"),
                        }

                        // Assert the second joining condition (connection to left node - standalone relationship)
                        let second_join_condition = &rel_join.joining_on[1];
                        assert_eq!(second_join_condition.operator, Operator::Equal);
                        assert_eq!(second_join_condition.operands.len(), 2);

                        match (
                            &second_join_condition.operands[0],
                            &second_join_condition.operands[1],
                        ) {
                            (
                                LogicalExpr::PropertyAccessExp(rel_prop),
                                LogicalExpr::PropertyAccessExp(right_prop),
                            ) => {
                                assert_eq!(rel_prop.table_alias.0, "f2");
                                assert_eq!(rel_prop.column.0, "from_id");
                                assert_eq!(right_prop.table_alias.0, "p3");
                                assert_eq!(right_prop.column.0, "id");
                            }
                            _ => panic!("Expected PropertyAccessExp operands"),
                        }
                    }
                    _ => panic!("Expected GraphJoins node"),
                }
            }
            _ => panic!("Expected transformation"),
        }
    }

    #[test]
    fn test_incoming_direction_edge_list() {
        let analyzer = GraphJoinInference::new();
        let graph_schema = create_test_graph_schema();
        let mut plan_ctx = setup_plan_ctx_with_graph_entities();

        // Update relationship label for incoming direction
        // plan_ctx.get_mut_table_ctx("f1").unwrap().set_label(Some("FOLLOWS_incoming".to_string()));
        plan_ctx
            .get_mut_table_ctx("f1")
            .unwrap()
            .set_use_edge_list(true);

        // Create plan: (p1)<-[f1:FOLLOWS]-(p2)
        let p1_scan = create_scan_plan("p1", "Person");
        let p1_node = create_graph_node(p1_scan, "p1");

        let f1_scan = create_scan_plan("f1", "FOLLOWS");

        let p2_scan = create_scan_plan("p2", "Person");
        let p2_node = create_graph_node(p2_scan, "p2");

        let graph_rel = create_graph_rel(
            p2_node,
            f1_scan,
            p1_node,
            "f1",
            Direction::Incoming,
            "p2",
            "p1",
        );
        let input_logical_plan = Arc::new(LogicalPlan::Projection(Projection {
            input: graph_rel,
            items: vec![ProjectionItem {
                expression: LogicalExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias("p1".to_string()),
                    column: Column("name".to_string()),
                }),
                col_alias: None,
            }],
        }));

        let result = analyzer
            .analyze_with_graph_schema(input_logical_plan, &mut plan_ctx, &graph_schema)
            .unwrap();

        // Should create appropriate joins for incoming direction
        match result {
            Transformed::Yes(plan) => {
                match plan.as_ref() {
                    LogicalPlan::GraphJoins(graph_joins) => {
                        // Assert GraphJoins structure
                        assert_eq!(graph_joins.joins.len(), 2);
                        assert!(matches!(
                            graph_joins.input.as_ref(),
                            LogicalPlan::Projection(_)
                        ));

                        // Verify that joins are created (specific join conditions depend on direction)
                        let rel_join = &graph_joins.joins[0];
                        assert_eq!(rel_join.table_name, "FOLLOWS_f1");
                        assert_eq!(rel_join.table_alias, "f1");
                        assert_eq!(rel_join.join_type, JoinType::Inner);
                        assert_eq!(rel_join.joining_on.len(), 1);

                        // Assert the joining condition for relationship (incoming direction)
                        let rel_join_condition = &rel_join.joining_on[0];
                        assert_eq!(rel_join_condition.operator, Operator::Equal);
                        assert_eq!(rel_join_condition.operands.len(), 2);

                        // For incoming direction, the relationship connects differently
                        match (
                            &rel_join_condition.operands[0],
                            &rel_join_condition.operands[1],
                        ) {
                            (
                                LogicalExpr::PropertyAccessExp(rel_prop),
                                LogicalExpr::PropertyAccessExp(left_prop),
                            ) => {
                                assert_eq!(rel_prop.table_alias.0, "f1");
                                assert_eq!(rel_prop.column.0, "from_id");
                                assert_eq!(left_prop.table_alias.0, "p2");
                                assert_eq!(left_prop.column.0, "id");
                            }
                            _ => panic!("Expected PropertyAccessExp operands"),
                        }

                        let right_join = &graph_joins.joins[1];
                        assert_eq!(right_join.table_name, "Person_p1");
                        assert_eq!(right_join.table_alias, "p1");
                        assert_eq!(right_join.join_type, JoinType::Inner);
                        assert_eq!(right_join.joining_on.len(), 1);

                        // Assert the joining condition for right node
                        let right_join_condition = &right_join.joining_on[0];
                        assert_eq!(right_join_condition.operator, Operator::Equal);
                        assert_eq!(right_join_condition.operands.len(), 2);

                        match (
                            &right_join_condition.operands[0],
                            &right_join_condition.operands[1],
                        ) {
                            (
                                LogicalExpr::PropertyAccessExp(left_prop),
                                LogicalExpr::PropertyAccessExp(right_prop),
                            ) => {
                                assert_eq!(left_prop.table_alias.0, "p1");
                                assert_eq!(left_prop.column.0, "id");
                                assert_eq!(right_prop.table_alias.0, "f1");
                                assert_eq!(right_prop.column.0, "to_id"); // or from_id based on incoming direction
                            }
                            _ => panic!("Expected PropertyAccessExp operands"),
                        }
                    }
                    _ => panic!("Expected GraphJoins node"),
                }
            }
            _ => panic!("Expected transformation"),
        }
    }

    #[test]
    fn test_complex_nested_plan_with_multiple_graph_rels() {
        let analyzer = GraphJoinInference::new();
        let graph_schema = create_test_graph_schema();
        let mut plan_ctx = setup_plan_ctx_with_graph_entities();

        // Set relationships to use edge list
        plan_ctx
            .get_mut_table_ctx("f1")
            .unwrap()
            .set_use_edge_list(true);
        plan_ctx
            .get_mut_table_ctx("w1")
            .unwrap()
            .set_use_edge_list(true);

        // Create complex plan: (p1)-[f1:FOLLOWS]->(p2)-[w1:WORKS_AT]->(c1)
        let p1_scan = create_scan_plan("p1", "Person");
        let p1_node = create_graph_node(p1_scan, "p1");

        let f1_scan = create_scan_plan("f1", "FOLLOWS");

        let p2_scan = create_scan_plan("p2", "Person");
        let p2_node = create_graph_node(p2_scan, "p2");

        let first_rel = create_graph_rel(
            p2_node,
            f1_scan,
            p1_node,
            "f1",
            Direction::Outgoing,
            "p2",
            "p1",
        );

        let w1_scan = create_scan_plan("w1", "WORKS_AT");

        let c1_scan = create_scan_plan("c1", "Company");
        let c1_node = create_graph_node(c1_scan, "c1");

        // (p1)-[f1:FOLLOWS]->(p2)-[w1:WORKS_AT]->(c1)

        let second_rel = create_graph_rel(
            c1_node,
            w1_scan,
            first_rel,
            "w1",
            Direction::Outgoing,
            "c1",
            "p2",
        );

        let input_logical_plan = Arc::new(LogicalPlan::Projection(Projection {
            input: second_rel,
            items: vec![ProjectionItem {
                expression: LogicalExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias("p1".to_string()),
                    column: Column("name".to_string()),
                }),
                col_alias: None,
            }],
        }));

        let result = analyzer
            .analyze_with_graph_schema(input_logical_plan, &mut plan_ctx, &graph_schema)
            .unwrap();

        // (p1)-[f1:FOLLOWS]->(p2)-[w1:WORKS_AT]->(c1)
        // In this case, c1 is the ending node, we are now joining in reverse order.
        // It means first we will join c1 -> w1, w1 -> p2, p2 -> f1, f1 -> p1.
        // So the tables in the order of joining will be w1, p2, f1, p1.
        // Note that c1 is not a part of the join, it is just the ending node.

        // Should create joins for all relationships in the chain
        match result {
            Transformed::Yes(plan) => {
                match plan.as_ref() {
                    LogicalPlan::GraphJoins(graph_joins) => {
                        // Assert GraphJoins structure
                        assert!(graph_joins.joins.len() >= 2);
                        assert!(matches!(
                            graph_joins.input.as_ref(),
                            LogicalPlan::Projection(_)
                        ));

                        // Verify we have joins for both relationship aliases
                        let rel_aliases: Vec<&String> =
                            graph_joins.joins.iter().map(|j| &j.table_alias).collect();

                        // Should contain joins for both relationships
                        assert!(
                            rel_aliases
                                .iter()
                                .any(|&alias| alias == "f1" || alias == "w1")
                        );

                        // Should have joins for both relationships in the chain: (p1)-[f1:FOLLOWS]->(p2)-[w1:WORKS_AT]->(c1)
                        assert!(graph_joins.joins.len() == 4); // 4 joins for two relationships with their nodes

                        // Verify we have the expected join aliases for the new structure: (p1)-[f1:FOLLOWS]->(p2)-[w1:WORKS_AT]->(c1)
                        let join_aliases: Vec<&String> =
                            graph_joins.joins.iter().map(|j| &j.table_alias).collect();

                        println!("Join aliases found: {:?}", join_aliases);
                        assert!(join_aliases.contains(&&"w1".to_string()));
                        assert!(join_aliases.contains(&&"p2".to_string()));
                        assert!(join_aliases.contains(&&"f1".to_string()));
                        assert!(join_aliases.contains(&&"p1".to_string()));

                        // Verify each join has the correct structure
                        for join in &graph_joins.joins {
                            assert_eq!(join.join_type, JoinType::Inner);
                            assert!(!join.joining_on.is_empty());

                            // (p1)-[f1:FOLLOWS]->(p2)-[w1:WORKS_AT]->(c1)
                            // Join order = c1 -> w1, w1 -> p2, p2 -> f1, f1 -> p1.
                            // Verify specific join details based on alias
                            match join.table_alias.as_str() {
                                "w1" => {
                                    assert_eq!(join.table_name, "WORKS_AT_w1");
                                    assert_eq!(join.joining_on.len(), 1);

                                    let join_condition = &join.joining_on[0];
                                    assert_eq!(join_condition.operator, Operator::Equal);
                                    assert_eq!(join_condition.operands.len(), 2);

                                    println!("Join condition: {:?}", join_condition);

                                    // Verify the join condition connects w1 with c1 (c1 is the left side) and w1.to_id connects to p2
                                    match (&join_condition.operands[0], &join_condition.operands[1])
                                    {
                                        (
                                            LogicalExpr::PropertyAccessExp(rel_prop),
                                            LogicalExpr::PropertyAccessExp(left_prop),
                                        ) => {
                                            assert_eq!(rel_prop.table_alias.0, "w1");
                                            assert_eq!(rel_prop.column.0, "to_id");
                                            assert_eq!(left_prop.table_alias.0, "c1");
                                            assert_eq!(left_prop.column.0, "id");
                                        }
                                        _ => panic!(
                                            "Expected PropertyAccessExp operands for w1 join"
                                        ),
                                    }
                                }
                                "p2" => {
                                    assert_eq!(join.table_name, "Person_p2");
                                    assert_eq!(join.joining_on.len(), 1);

                                    let join_condition = &join.joining_on[0];
                                    assert_eq!(join_condition.operator, Operator::Equal);
                                    assert_eq!(join_condition.operands.len(), 2);

                                    // Verify the join condition connects p2 with f1
                                    match (&join_condition.operands[0], &join_condition.operands[1])
                                    {
                                        (
                                            LogicalExpr::PropertyAccessExp(left_prop),
                                            LogicalExpr::PropertyAccessExp(rel_prop),
                                        ) => {
                                            assert_eq!(left_prop.table_alias.0, "p2");
                                            assert_eq!(left_prop.column.0, "id");
                                            assert_eq!(rel_prop.table_alias.0, "w1");
                                            assert_eq!(rel_prop.column.0, "from_id");
                                        }
                                        _ => panic!(
                                            "Expected PropertyAccessExp operands for p2 join"
                                        ),
                                    }
                                }
                                "f1" => {
                                    assert_eq!(join.table_name, "FOLLOWS_f1");
                                    assert_eq!(join.joining_on.len(), 1);

                                    let join_condition = &join.joining_on[0];
                                    assert_eq!(join_condition.operator, Operator::Equal);
                                    assert_eq!(join_condition.operands.len(), 2);

                                    // Verify the join condition connects f1 with p1
                                    match (&join_condition.operands[0], &join_condition.operands[1])
                                    {
                                        (
                                            LogicalExpr::PropertyAccessExp(rel_prop),
                                            LogicalExpr::PropertyAccessExp(left_prop),
                                        ) => {
                                            assert_eq!(rel_prop.table_alias.0, "f1");
                                            assert_eq!(rel_prop.column.0, "to_id");
                                            assert_eq!(left_prop.table_alias.0, "p2");
                                            assert_eq!(left_prop.column.0, "id");
                                        }
                                        _ => panic!(
                                            "Expected PropertyAccessExp operands for f1 join"
                                        ),
                                    }
                                }
                                "p1" => {
                                    assert_eq!(join.table_name, "Person_p1");
                                    assert_eq!(join.joining_on.len(), 1);

                                    let join_condition = &join.joining_on[0];
                                    assert_eq!(join_condition.operator, Operator::Equal);
                                    assert_eq!(join_condition.operands.len(), 2);

                                    // Verify the join condition connects p1 with f1
                                    match (&join_condition.operands[0], &join_condition.operands[1])
                                    {
                                        (
                                            LogicalExpr::PropertyAccessExp(left_prop),
                                            LogicalExpr::PropertyAccessExp(rel_prop),
                                        ) => {
                                            assert_eq!(left_prop.table_alias.0, "p1");
                                            assert_eq!(left_prop.column.0, "id");
                                            assert_eq!(rel_prop.table_alias.0, "f1");
                                            assert_eq!(rel_prop.column.0, "from_id");
                                        }
                                        _ => panic!(
                                            "Expected PropertyAccessExp operands for p1 join"
                                        ),
                                    }
                                }
                                _ => {
                                    // Allow other joins but ensure they have basic structure
                                    assert!(!join.table_name.is_empty());
                                    for join_condition in &join.joining_on {
                                        assert_eq!(join_condition.operator, Operator::Equal);
                                        assert_eq!(join_condition.operands.len(), 2);
                                    }
                                }
                            }
                        }
                    }
                    _ => panic!("Expected GraphJoins node"),
                }
            }
            _ => panic!("Expected transformation"),
        }
    }
}
