use std::{collections::HashSet, sync::Arc};

use crate::{ graph_schema::graph_schema::GraphSchema, query_planner::{analyzer::{analyzer_pass::{AnalyzerPass, AnalyzerResult}, errors::{AnalyzerError, Pass}}, logical_expr::logical_expr::{Column, Direction, LogicalExpr, Operator, OperatorApplication, PropertyAccess, TableAlias}, logical_plan::logical_plan::{GraphJoins, GraphRel, Join, LogicalPlan}, plan_ctx::plan_ctx::PlanCtx, transformed::Transformed}};







pub struct GraphJoinInference;

impl AnalyzerPass for GraphJoinInference {
    fn analyze_with_graph_schema(&self, logical_plan: Arc<LogicalPlan>, plan_ctx: &mut PlanCtx, graph_schema: &GraphSchema) -> AnalyzerResult<Transformed<Arc<LogicalPlan>>> {

        let mut collected_graph_joins:Vec<Join> = vec![];
        let mut joined_entities: HashSet<String> = HashSet::new();
        self.collect_graph_joins(logical_plan.clone(), plan_ctx, graph_schema, &mut collected_graph_joins, &mut joined_entities)?;
        if !collected_graph_joins.is_empty() {
            self.build_graph_joins(logical_plan, &mut collected_graph_joins)
        } else {
            Ok(Transformed::No(logical_plan.clone()))
        }

    }
}


impl GraphJoinInference {
    pub fn new() -> Self {
        GraphJoinInference
    }

    fn build_graph_joins(&self, logical_plan: Arc<LogicalPlan>, collected_graph_joins: &mut Vec<Join>) -> AnalyzerResult<Transformed<Arc<LogicalPlan>>> {
        let transformed_plan = match logical_plan.as_ref() {
            LogicalPlan::Projection(_) => {
                // wrap the outer projection i.e. first occurance in the tree walk with Graph joins
                Transformed::Yes(Arc::new(LogicalPlan::GraphJoins(GraphJoins{
                    input: logical_plan.clone(),
                    joins: collected_graph_joins.to_vec(),
                })))
            },
            LogicalPlan::GraphNode(graph_node) => {
                let child_tf = self.build_graph_joins(graph_node.input.clone(), collected_graph_joins)?;
                graph_node.rebuild_or_clone(child_tf, logical_plan.clone())
            },
            LogicalPlan::GraphRel(graph_rel) => {
                let left_tf = self.build_graph_joins(graph_rel.left.clone(), collected_graph_joins)?;
                let center_tf = self.build_graph_joins(graph_rel.center.clone(), collected_graph_joins)?;
                let right_tf = self.build_graph_joins(graph_rel.right.clone(), collected_graph_joins)?;

                graph_rel.rebuild_or_clone(left_tf, center_tf, right_tf, logical_plan.clone())
            },
            LogicalPlan::Cte(cte   ) => {
                let child_tf = self.build_graph_joins( cte.input.clone(), collected_graph_joins)?;
                cte.rebuild_or_clone(child_tf, logical_plan.clone())
            },
            LogicalPlan::Scan(_) => {
                Transformed::No(logical_plan.clone())
            },
            LogicalPlan::Empty => Transformed::No(logical_plan.clone()),
            LogicalPlan::GraphJoins(graph_joins) => {
                let child_tf = self.build_graph_joins(graph_joins.input.clone(), collected_graph_joins)?;
                graph_joins.rebuild_or_clone(child_tf, logical_plan.clone())
            },
            LogicalPlan::Filter(filter) => {
                let child_tf = self.build_graph_joins(filter.input.clone(), collected_graph_joins)?;
                filter.rebuild_or_clone(child_tf, logical_plan.clone())
            },
            LogicalPlan::GroupBy(group_by   ) => {
                let child_tf = self.build_graph_joins(group_by.input.clone(), collected_graph_joins)?;
                group_by.rebuild_or_clone(child_tf, logical_plan.clone())
            },
            LogicalPlan::OrderBy(order_by) => {
                let child_tf = self.build_graph_joins(order_by.input.clone(), collected_graph_joins)?;
                order_by.rebuild_or_clone(child_tf, logical_plan.clone())
            },
            LogicalPlan::Skip(skip) => {
                let child_tf = self.build_graph_joins(skip.input.clone(), collected_graph_joins)?;
                skip.rebuild_or_clone(child_tf, logical_plan.clone())
            },
            LogicalPlan::Limit(limit) => {
                let child_tf = self.build_graph_joins(limit.input.clone(), collected_graph_joins)?;
                limit.rebuild_or_clone(child_tf, logical_plan.clone())
            },
            LogicalPlan::Union(union) => {
                let mut inputs_tf: Vec<Transformed<Arc<LogicalPlan>>> = vec![];
                for input_plan in union.inputs.iter() {
                    let child_tf = self.build_graph_joins(input_plan.clone(), collected_graph_joins)?; 
                    inputs_tf.push(child_tf);
                }
                union.rebuild_or_clone(inputs_tf, logical_plan.clone())
            },
        };
        Ok(transformed_plan)
    }

    fn collect_graph_joins(&self, logical_plan: Arc<LogicalPlan>, plan_ctx: &mut PlanCtx, graph_schema: &GraphSchema, collected_graph_joins: &mut Vec<Join>, joined_entities: &mut HashSet<String>) -> AnalyzerResult<()> {
        match logical_plan.as_ref() {
            LogicalPlan::Projection(projection) => {
                self.collect_graph_joins(projection.input.clone(), plan_ctx, graph_schema, collected_graph_joins, joined_entities)
            },
            LogicalPlan::GraphNode(graph_node) => {
                self.collect_graph_joins(graph_node.input.clone(), plan_ctx, graph_schema, collected_graph_joins, joined_entities)
            },
            LogicalPlan::GraphRel(graph_rel) => {
                // infer joins for each graph_rel

                self.infer_graph_join(graph_rel, plan_ctx, graph_schema, collected_graph_joins, joined_entities)?;

                // self.collect_graph_joins(graph_rel.left.clone(), plan_ctx, graph_schema, collected_graph_joins, joined_entities);
                // self.collect_graph_joins(graph_rel.center.clone(), plan_ctx, graph_schema, collected_graph_joins, joined_entities);
                self.collect_graph_joins(graph_rel.right.clone(), plan_ctx, graph_schema, collected_graph_joins, joined_entities)
            },
            LogicalPlan::Cte(cte   ) => {
                self.collect_graph_joins( cte.input.clone(), plan_ctx, graph_schema, collected_graph_joins, joined_entities)
            },
            LogicalPlan::Scan(_) => Ok(()),
            LogicalPlan::Empty => Ok(()),
            LogicalPlan::GraphJoins(graph_joins) => {
                self.collect_graph_joins(graph_joins.input.clone(), plan_ctx, graph_schema, collected_graph_joins, joined_entities)
            },
            LogicalPlan::Filter(filter) => {
                self.collect_graph_joins(filter.input.clone(), plan_ctx, graph_schema, collected_graph_joins, joined_entities)
            },
            LogicalPlan::GroupBy(group_by   ) => {
                self.collect_graph_joins(group_by.input.clone(), plan_ctx, graph_schema, collected_graph_joins, joined_entities)
            },
            LogicalPlan::OrderBy(order_by) => {
                self.collect_graph_joins(order_by.input.clone(), plan_ctx, graph_schema, collected_graph_joins, joined_entities)
            },
            LogicalPlan::Skip(skip) => {
                self.collect_graph_joins(skip.input.clone(), plan_ctx, graph_schema, collected_graph_joins, joined_entities)
            },
            LogicalPlan::Limit(limit) => {
                self.collect_graph_joins(limit.input.clone(), plan_ctx, graph_schema, collected_graph_joins, joined_entities)
            },
            LogicalPlan::Union(union) => {
                for input_plan in union.inputs.iter() {
                    self.collect_graph_joins(input_plan.clone(), plan_ctx, graph_schema, collected_graph_joins, joined_entities)?; 
                }
                Ok(())
            },
        }
    }

    fn infer_graph_join(&self, graph_rel: &GraphRel, plan_ctx: &mut PlanCtx, graph_schema: &GraphSchema, collected_graph_joins: &mut Vec<Join>, joined_entities: &mut HashSet<String>) -> AnalyzerResult<()> {
        // get required information 
        let left_alias = &graph_rel.left_connection;
        let rel_alias = &graph_rel.alias;
        let right_alias = &graph_rel.right_connection;


        let left_ctx = plan_ctx.get_node_table_ctx(left_alias).map_err(|e| AnalyzerError::PlanCtx { pass: Pass::GraphJoinInference, source: e})?;
        let rel_ctx = plan_ctx.get_rel_table_ctx(rel_alias).map_err(|e| AnalyzerError::PlanCtx { pass: Pass::GraphJoinInference, source: e})?;
        let right_ctx = plan_ctx.get_node_table_ctx(right_alias).map_err(|e| AnalyzerError::PlanCtx { pass: Pass::GraphJoinInference, source: e})?;

        let left_label = left_ctx.get_label_str().map_err(|e| AnalyzerError::PlanCtx { pass: Pass::GraphJoinInference, source: e})?;
        let rel_label = rel_ctx.get_label_str().map_err(|e| AnalyzerError::PlanCtx { pass: Pass::GraphJoinInference, source: e})?;
        let original_rel_label = rel_label.replace(format!("_{}", Direction::Incoming).as_str(), "").replace(format!("_{}", Direction::Outgoing).as_str(), "").replace(format!("_{}", Direction::Either).as_str(), "");
        let right_label = right_ctx.get_label_str().map_err(|e| AnalyzerError::PlanCtx { pass: Pass::GraphJoinInference, source: e})?;

        let left_schema = graph_schema.get_node_schema(&left_label).map_err(|e| AnalyzerError::GraphSchema { pass: Pass::GraphJoinInference, source: e})?;
        let rel_schema = graph_schema.get_rel_schema(&original_rel_label).map_err(|e| AnalyzerError::GraphSchema { pass: Pass::GraphJoinInference, source: e})?;
        let right_schema = graph_schema.get_node_schema(&right_label).map_err(|e| AnalyzerError::GraphSchema { pass: Pass::GraphJoinInference, source: e})?;

        let rel_cte_name = format!("{}_{}", rel_label, rel_alias);
        let right_cte_name = format!("{}_{}", right_label, right_alias);
        let left_cte_name = format!("{}_{}", left_label, left_alias);

        let left_node_id_column = left_schema.node_id.column.clone();
        let right_node_id_column = right_schema.node_id.column.clone();   

        // Check for standalone relationship join. 
        // e.g. MATCH (a)-[f1:Follows]->(b)-[f2:Follows]->(c), (a)-[f3:Follows]->(c)
        // In the duplicate scan removing pass, we remove the already scanned nodes. We do this from bottom to up.
        // So there could be a graph_rel who has LogicalPlan::Empty as left. In such case just join the relationship but on both nodes columns.
        // In case of f3, both of its nodes a and b are already joined. So just join f3 on both a and b's joining keys.
        let is_standalone_rel: bool = matches!(graph_rel.left.as_ref(), LogicalPlan::Empty);


        if rel_ctx.should_use_edge_list() {
            // If both nodes are of the same type then check the direction to determine where are the left and right nodes present in the edgelist.
            if left_schema.table_name == right_schema.table_name {
                if joined_entities.contains(right_alias) {
                    // join the rel with right first and then join the left with rel
                    let (rel_conn_with_right_node, left_conn_with_rel) = if graph_rel.direction == Direction::Incoming {
                        ("from_id".to_string(), "to_id".to_string())
                    } else {
                        ("to_id".to_string(), "from_id".to_string())
                    };
                    let mut rel_graph_join = Join{
                        table_name: rel_cte_name,
                        table_alias: rel_alias.to_string(),
                        joining_on: vec![
                            OperatorApplication{
                                operator: Operator::Equal,
                                operands: vec![
                                    LogicalExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(rel_alias.to_string()), column: Column(rel_conn_with_right_node) }),
                                    LogicalExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(right_alias.to_string()), column: Column(right_node_id_column.clone()) })
                                ]
                            }
                        ],
                    };

                    let left_graph_join = Join {
                        table_name: left_cte_name,
                        table_alias: left_alias.to_string(),
                        joining_on: vec![
                            OperatorApplication{
                                operator: Operator::Equal,
                                operands: vec![
                                    LogicalExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(left_alias.to_string()), column: Column(left_node_id_column.clone()) }),
                                    LogicalExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(rel_alias.to_string()), column: Column(left_conn_with_rel.clone()) })
                                ]
                            }
                        ],
                    };

                    if is_standalone_rel {
                        let rel_to_right_graph_join_keys = 
                            OperatorApplication{
                                operator: Operator::Equal,
                                operands: vec![
                                    LogicalExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(rel_alias.to_string()), column: Column(left_conn_with_rel) }),
                                    LogicalExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(left_alias.to_string()), column: Column(left_node_id_column) }),
                                ]
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

                    let (rel_conn_with_left_node, right_conn_with_rel) = if graph_rel.direction == Direction::Incoming {
                        ("from_id".to_string(), "to_id".to_string())
                    } else {
                        ("to_id".to_string(), "from_id".to_string())
                    };

                    let mut rel_graph_join = Join{
                        table_name: rel_cte_name,
                        table_alias: rel_alias.to_string(),
                        joining_on: vec![
                            OperatorApplication{
                                operator: Operator::Equal,
                                operands: vec![
                                    LogicalExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(rel_alias.to_string()), column: Column(rel_conn_with_left_node) }),
                                    LogicalExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(left_alias.to_string()), column: Column(left_node_id_column.clone()) })
                                ]
                            }
                        ],
                    };

                    let right_graph_join = Join {
                        table_name: right_cte_name,
                        table_alias: right_alias.to_string(),
                        joining_on: vec![
                            OperatorApplication{
                                operator: Operator::Equal,
                                operands: vec![
                                    LogicalExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(right_alias.to_string()), column: Column(right_node_id_column.clone()) }),
                                    LogicalExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(rel_alias.to_string()), column: Column(right_conn_with_rel.clone()) })
                                ]
                            }
                        ],
                    };

                    if is_standalone_rel {
                        let rel_to_right_graph_join_keys = 
                            OperatorApplication{
                                operator: Operator::Equal,
                                operands: vec![
                                    LogicalExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(rel_alias.to_string()), column: Column(right_conn_with_rel) }),
                                    LogicalExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(right_alias.to_string()), column: Column(right_node_id_column) }),
                                ]
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
            if rel_schema.from_node == right_schema.table_name {
                // this means rel.from_node = right and to_node = left

                // check if right is already joined 
                if joined_entities.contains(right_alias) {
                    // join the rel with right first and then join the left with rel
                    let mut rel_graph_join = Join{
                        table_name: rel_cte_name,
                        table_alias: rel_alias.to_string(),
                        joining_on: vec![
                            OperatorApplication{
                                operator: Operator::Equal,
                                operands: vec![
                                    LogicalExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(rel_alias.to_string()), column: Column("from_id".to_string()) }),
                                    LogicalExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(right_alias.to_string()), column: Column(right_node_id_column.clone()) })
                                ]
                            }
                        ],
                    };

                    let left_graph_join = Join {
                        table_name: left_cte_name,
                        table_alias: left_alias.to_string(),
                        joining_on: vec![
                            OperatorApplication{
                                operator: Operator::Equal,
                                operands: vec![
                                    LogicalExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(left_alias.to_string()), column: Column(left_node_id_column.clone()) }),
                                    LogicalExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(rel_alias.to_string()), column: Column("to_id".to_string()) })
                                ]
                            }
                        ],
                    };

                    if is_standalone_rel {
                        let rel_to_right_graph_join_keys = 
                            OperatorApplication{
                                operator: Operator::Equal,
                                operands: vec![
                                    LogicalExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(rel_alias.to_string()), column: Column("to_id".to_string()) }),
                                    LogicalExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(left_alias.to_string()), column: Column(left_node_id_column) }),
                                ]
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
                    let mut rel_graph_join = Join{
                        table_name: rel_cte_name,
                        table_alias: rel_alias.to_string(),
                        joining_on: vec![
                            OperatorApplication{
                                operator: Operator::Equal,
                                operands: vec![
                                    LogicalExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(rel_alias.to_string()), column: Column("to_id".to_string()) }),
                                    LogicalExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(left_alias.to_string()), column: Column(left_node_id_column.clone()) })
                                ]
                            }
                        ],
                    };

                    let right_graph_join = Join {
                        table_name: right_cte_name,
                        table_alias: right_alias.to_string(),
                        joining_on: vec![
                            OperatorApplication{
                                operator: Operator::Equal,
                                operands: vec![
                                    LogicalExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(right_alias.to_string()), column: Column(right_node_id_column.clone()) }),
                                    LogicalExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(rel_alias.to_string()), column: Column("from_id".to_string()) })
                                ]
                            }
                        ],
                    };

                    if is_standalone_rel {
                        let rel_to_right_graph_join_keys = 
                            OperatorApplication{
                                operator: Operator::Equal,
                                operands: vec![
                                    LogicalExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(rel_alias.to_string()), column: Column("from_id".to_string()) }),
                                    LogicalExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(right_alias.to_string()), column: Column(right_node_id_column) }),
                                ]
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
                    let mut rel_graph_join = Join{
                        table_name: rel_cte_name,
                        table_alias: rel_alias.to_string(),
                        joining_on: vec![
                            OperatorApplication{
                                operator: Operator::Equal,
                                operands: vec![
                                    LogicalExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(rel_alias.to_string()), column: Column("to_id".to_string()) }),
                                    LogicalExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(right_alias.to_string()), column: Column(right_node_id_column.clone()) })
                                ]
                            }
                        ],
                    };

                    let left_graph_join = Join {
                        table_name: left_cte_name,
                        table_alias: left_alias.to_string(),
                        joining_on: vec![
                            OperatorApplication{
                                operator: Operator::Equal,
                                operands: vec![
                                    LogicalExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(left_alias.to_string()), column: Column(left_node_id_column.clone()) }),
                                    LogicalExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(rel_alias.to_string()), column: Column("from_id".to_string()) })
                                ]
                            }
                        ],
                    };

                    if is_standalone_rel {
                        let rel_to_right_graph_join_keys = 
                            OperatorApplication{
                                operator: Operator::Equal,
                                operands: vec![
                                    LogicalExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(rel_alias.to_string()), column: Column("from_id".to_string()) }),
                                    LogicalExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(left_alias.to_string()), column: Column(left_node_id_column) }),
                                ]
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
                    let mut rel_graph_join = Join{
                        table_name: rel_cte_name,
                        table_alias: rel_alias.to_string(),
                        joining_on: vec![
                            OperatorApplication{
                                operator: Operator::Equal,
                                operands: vec![
                                    LogicalExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(rel_alias.to_string()), column: Column("from_id".to_string()) }),
                                    LogicalExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(left_alias.to_string()), column: Column(left_node_id_column.clone()) })
                                ]
                            }
                        ],
                    };

                    let right_graph_join = Join {
                        table_name: right_cte_name,
                        table_alias: right_alias.to_string(),
                        joining_on: vec![
                            OperatorApplication{
                                operator: Operator::Equal,
                                operands: vec![
                                    LogicalExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(right_alias.to_string()), column: Column(right_node_id_column.clone()) }),
                                    LogicalExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(rel_alias.to_string()), column: Column("to_id".to_string()) })
                                ]
                            }
                        ],
                    };

                    if is_standalone_rel {
                        let rel_to_right_graph_join_keys = 
                            OperatorApplication{
                                operator: Operator::Equal,
                                operands: vec![
                                    LogicalExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(rel_alias.to_string()), column: Column("to_id".to_string()) }),
                                    LogicalExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(right_alias.to_string()), column: Column(right_node_id_column) }),
                                ]
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

        } else {
            // check if right is alredy joined. 
            if joined_entities.contains(right_alias) {
                // join the rel with right first and then join the left with rel
                let mut rel_graph_join = Join{
                    table_name: rel_cte_name,
                    table_alias: rel_alias.to_string(),
                    joining_on: vec![
                        OperatorApplication{
                            operator: Operator::Equal,
                            operands: vec![
                                LogicalExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(rel_alias.to_string()), column: Column("from_id".to_string()) }),
                                LogicalExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(right_alias.to_string()), column: Column(right_node_id_column.clone()) })
                            ]
                        }
                    ],
                };
                
                let left_graph_join = Join {
                    table_name: left_cte_name,
                    table_alias: left_alias.to_string(),
                    joining_on: vec![
                        OperatorApplication{
                            operator: Operator::Equal,
                            operands: vec![
                                LogicalExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(left_alias.to_string()), column: Column(left_node_id_column.clone()) }),
                                LogicalExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(rel_alias.to_string()), column: Column("to_id".to_string()) })
                            ]
                        }
                    ],
                };

                if is_standalone_rel {
                    let rel_to_right_graph_join_keys = 
                        OperatorApplication{
                            operator: Operator::Equal,
                            operands: vec![
                                LogicalExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(rel_alias.to_string()), column: Column("to_id".to_string()) }),
                                LogicalExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(left_alias.to_string()), column: Column(left_node_id_column) }),
                            ]
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
                let mut rel_graph_join = Join{
                    table_name: rel_cte_name,
                    table_alias: rel_alias.to_string(),
                    joining_on: vec![
                        OperatorApplication{
                            operator: Operator::Equal,
                            operands: vec![
                                LogicalExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(rel_alias.to_string()), column: Column("to_id".to_string()) }),
                                LogicalExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(left_alias.to_string()), column: Column(left_node_id_column.clone()) })
                            ]
                        }
                    ],
                };

                let right_graph_join = Join {
                    table_name: right_cte_name,
                    table_alias: right_alias.to_string(),
                    joining_on: vec![
                        OperatorApplication{
                            operator: Operator::Equal,
                            operands: vec![
                                LogicalExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(right_alias.to_string()), column: Column(right_node_id_column.clone()) }),
                                LogicalExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(rel_alias.to_string()), column: Column("from_id".to_string()) })
                            ]
                        }
                    ],
                };

                if is_standalone_rel {
                    let rel_to_right_graph_join_keys = 
                        OperatorApplication{
                            operator: Operator::Equal,
                            operands: vec![
                                LogicalExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(rel_alias.to_string()), column: Column("from_id".to_string()) }),
                                LogicalExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(right_alias.to_string()), column: Column(right_node_id_column) }),
                            ]
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

}
