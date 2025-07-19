use std::{collections::HashSet, sync::Arc};

use crate::{query_engine::types::GraphSchema, query_engine_v2::{analyzer::{analyzer_pass::AnalyzerPass, errors::AnalyzerError}, expr::plan_expr::{Column, Operator, OperatorApplication, PlanExpr, PropertyAccess, TableAlias}, logical_plan::{logical_plan::{GraphJoins, GraphRel, Join, LogicalPlan}, plan_ctx::PlanCtx}, transformed::Transformed}};







pub struct GraphJoinInference;

impl AnalyzerPass for GraphJoinInference {
    fn analyze_with_graph_schema(&self, logical_plan: Arc<LogicalPlan>, plan_ctx: &mut PlanCtx, graph_schema: &GraphSchema) -> Transformed<Arc<LogicalPlan>> {

        let mut collected_graph_joins:Vec<Join> = vec![];
        let mut joined_entities: HashSet<String> = HashSet::new();
        self.collect_graph_joins(logical_plan.clone(), plan_ctx, graph_schema, &mut collected_graph_joins, &mut joined_entities);
        if !collected_graph_joins.is_empty() {
            self.build_graph_joins(logical_plan, &mut collected_graph_joins)
        } else {
            Transformed::No(logical_plan.clone())
        }

    }
}


impl GraphJoinInference {
    pub fn new() -> Self {
        GraphJoinInference
    }

    fn build_graph_joins(&self, logical_plan: Arc<LogicalPlan>, collected_graph_joins: &mut Vec<Join>) -> Transformed<Arc<LogicalPlan>> {
        match logical_plan.as_ref() {
            LogicalPlan::Projection(_) => {
                // wrap the outer projection i.e. first occurance in the tree walk with Graph joins
                Transformed::Yes(Arc::new(LogicalPlan::GraphJoins(GraphJoins{
                    input: logical_plan.clone(),
                    joins: collected_graph_joins.to_vec(),
                })))
            },
            LogicalPlan::GraphNode(graph_node) => {
                let child_tf = self.build_graph_joins(graph_node.input.clone(), collected_graph_joins);
                graph_node.rebuild_or_clone(child_tf, logical_plan.clone())
            },
            LogicalPlan::GraphRel(graph_rel) => {
                let left_tf = self.build_graph_joins(graph_rel.left.clone(), collected_graph_joins);
                let center_tf = self.build_graph_joins(graph_rel.center.clone(), collected_graph_joins);
                let right_tf = self.build_graph_joins(graph_rel.right.clone(), collected_graph_joins);

                graph_rel.rebuild_or_clone(left_tf, center_tf, right_tf, logical_plan.clone())
            },
            LogicalPlan::Cte(cte   ) => {
                let child_tf = self.build_graph_joins( cte.input.clone(), collected_graph_joins);
                cte.rebuild_or_clone(child_tf, logical_plan.clone())
            },
            LogicalPlan::Scan(_) => {
                Transformed::No(logical_plan.clone())
            },
            LogicalPlan::Empty => Transformed::No(logical_plan.clone()),
            LogicalPlan::ConnectedTraversal(connected_traversal) => {
                let left_tf = self.build_graph_joins(connected_traversal.start_node.clone(), collected_graph_joins);
                let rel_tf = self.build_graph_joins(connected_traversal.relationship.clone(), collected_graph_joins);
                let right_tf = self.build_graph_joins(connected_traversal.end_node.clone(), collected_graph_joins);
                connected_traversal.rebuild_or_clone(left_tf, rel_tf, right_tf, logical_plan.clone())
            },
            LogicalPlan::GraphJoins(graph_joins) => {
                let child_tf = self.build_graph_joins(graph_joins.input.clone(), collected_graph_joins);
                graph_joins.rebuild_or_clone(child_tf, logical_plan.clone())
            },
            LogicalPlan::Filter(filter) => {
                let child_tf = self.build_graph_joins(filter.input.clone(), collected_graph_joins);
                filter.rebuild_or_clone(child_tf, logical_plan.clone())
            },
            LogicalPlan::GroupBy(group_by   ) => {
                let child_tf = self.build_graph_joins(group_by.input.clone(), collected_graph_joins);
                group_by.rebuild_or_clone(child_tf, logical_plan.clone())
            },
            LogicalPlan::OrderBy(order_by) => {
                let child_tf = self.build_graph_joins(order_by.input.clone(), collected_graph_joins);
                order_by.rebuild_or_clone(child_tf, logical_plan.clone())
            },
            LogicalPlan::Skip(skip) => {
                let child_tf = self.build_graph_joins(skip.input.clone(), collected_graph_joins);
                skip.rebuild_or_clone(child_tf, logical_plan.clone())
            },
            LogicalPlan::Limit(limit) => {
                let child_tf = self.build_graph_joins(limit.input.clone(), collected_graph_joins);
                limit.rebuild_or_clone(child_tf, logical_plan.clone())
            },
        }
    }

    fn collect_graph_joins(&self, logical_plan: Arc<LogicalPlan>, plan_ctx: &mut PlanCtx, graph_schema: &GraphSchema, collected_graph_joins: &mut Vec<Join>, joined_entities: &mut HashSet<String>) {
        match logical_plan.as_ref() {
            LogicalPlan::Projection(projection) => {
                self.collect_graph_joins(projection.input.clone(), plan_ctx, graph_schema, collected_graph_joins, joined_entities);
            },
            LogicalPlan::GraphNode(graph_node) => {
                self.collect_graph_joins(graph_node.input.clone(), plan_ctx, graph_schema, collected_graph_joins, joined_entities);
            },
            LogicalPlan::GraphRel(graph_rel) => {
                // infer joins for each graph_rel

                self.infer_graph_join_1(graph_rel, plan_ctx, graph_schema, collected_graph_joins, joined_entities);

                // self.collect_graph_joins(graph_rel.left.clone(), plan_ctx, graph_schema, collected_graph_joins, joined_entities);
                // self.collect_graph_joins(graph_rel.center.clone(), plan_ctx, graph_schema, collected_graph_joins, joined_entities);
                self.collect_graph_joins(graph_rel.right.clone(), plan_ctx, graph_schema, collected_graph_joins, joined_entities);
            },
            LogicalPlan::Cte(cte   ) => {
                self.collect_graph_joins( cte.input.clone(), plan_ctx, graph_schema, collected_graph_joins, joined_entities);
            },
            LogicalPlan::Scan(_) => (),
            LogicalPlan::Empty => (),
            LogicalPlan::ConnectedTraversal(connected_traversal) => {
                self.collect_graph_joins(connected_traversal.start_node.clone(), plan_ctx, graph_schema, collected_graph_joins, joined_entities);
                self.collect_graph_joins(connected_traversal.relationship.clone(), plan_ctx, graph_schema, collected_graph_joins, joined_entities);
                self.collect_graph_joins(connected_traversal.end_node.clone(), plan_ctx, graph_schema, collected_graph_joins, joined_entities);
            },
            LogicalPlan::GraphJoins(graph_joins) => {
                self.collect_graph_joins(graph_joins.input.clone(), plan_ctx, graph_schema, collected_graph_joins, joined_entities);
            },
            LogicalPlan::Filter(filter) => {
                self.collect_graph_joins(filter.input.clone(), plan_ctx, graph_schema, collected_graph_joins, joined_entities);
            },
            LogicalPlan::GroupBy(group_by   ) => {
                self.collect_graph_joins(group_by.input.clone(), plan_ctx, graph_schema, collected_graph_joins, joined_entities);
            },
            LogicalPlan::OrderBy(order_by) => {
                self.collect_graph_joins(order_by.input.clone(), plan_ctx, graph_schema, collected_graph_joins, joined_entities);
            },
            LogicalPlan::Skip(skip) => {
                self.collect_graph_joins(skip.input.clone(), plan_ctx, graph_schema, collected_graph_joins, joined_entities);
            },
            LogicalPlan::Limit(limit) => {
                self.collect_graph_joins(limit.input.clone(), plan_ctx, graph_schema, collected_graph_joins, joined_entities);
            },
        }
    }
    
    // fn collect_graph_joins(&self, logical_plan: Arc<LogicalPlan>, plan_ctx: &mut PlanCtx, graph_schema: &GraphSchema, collected_graph_joins: &mut Vec<Join>) -> Transformed<Arc<LogicalPlan>> {
    //     match logical_plan.as_ref() {
    //         LogicalPlan::Projection(projection) => {
    //             let child_tf = self.collect_graph_joins(projection.input.clone(), plan_ctx, graph_schema, collected_graph_joins);
    //             projection.rebuild_or_clone(child_tf, logical_plan.clone())
    //         },
    //         LogicalPlan::GraphNode(graph_node) => {
    //             let child_tf = self.collect_graph_joins(graph_node.input.clone(), plan_ctx, graph_schema, collected_graph_joins);
    //             // let self_tf = self.collect_graph_joins(graph_node.self_plan.clone(), plan_ctx, graph_schema);
    //             graph_node.rebuild_or_clone(child_tf, logical_plan.clone())
    //         },
    //         LogicalPlan::GraphRel(graph_rel) => {
    //             // infer joins for each graph_rel

    //             self.infer_graph_join(graph_rel, plan_ctx, graph_schema, collected_graph_joins);

    //             let left_tf = self.collect_graph_joins(graph_rel.left.clone(), plan_ctx, graph_schema, collected_graph_joins);
    //             let center_tf = self.collect_graph_joins(graph_rel.center.clone(), plan_ctx, graph_schema, collected_graph_joins);
    //             let right_tf = self.collect_graph_joins(graph_rel.right.clone(), plan_ctx, graph_schema, collected_graph_joins);

    //             graph_rel.rebuild_or_clone(left_tf, center_tf, right_tf, logical_plan.clone())
    //         },
    //         LogicalPlan::Cte(cte   ) => {
    //             let child_tf = self.collect_graph_joins( cte.input.clone(), plan_ctx, graph_schema, collected_graph_joins);
    //             cte.rebuild_or_clone(child_tf, logical_plan.clone())
    //         },
    //         LogicalPlan::Scan(_) => {
    //             Transformed::No(logical_plan.clone())
    //         },
    //         LogicalPlan::Empty => Transformed::No(logical_plan.clone()),
    //         LogicalPlan::ConnectedTraversal(connected_traversal) => {
    //             let left_tf = self.collect_graph_joins(connected_traversal.start_node.clone(), plan_ctx, graph_schema, collected_graph_joins);
    //             let rel_tf = self.collect_graph_joins(connected_traversal.relationship.clone(), plan_ctx, graph_schema, collected_graph_joins);
    //             let right_tf = self.collect_graph_joins(connected_traversal.end_node.clone(), plan_ctx, graph_schema, collected_graph_joins);
    //             connected_traversal.rebuild_or_clone(left_tf, rel_tf, right_tf, logical_plan.clone())
    //         },
    //         LogicalPlan::GraphJoins(graph_joins) => {
    //             let child_tf = self.collect_graph_joins(graph_joins.input.clone(), plan_ctx, graph_schema, collected_graph_joins);
    //             graph_joins.rebuild_or_clone(child_tf, logical_plan.clone())
    //         },
    //         LogicalPlan::Filter(filter) => {
    //             let child_tf = self.collect_graph_joins(filter.input.clone(), plan_ctx, graph_schema, collected_graph_joins);
    //             filter.rebuild_or_clone(child_tf, logical_plan.clone())
    //         },
    //         LogicalPlan::GroupBy(group_by   ) => {
    //             let child_tf = self.collect_graph_joins(group_by.input.clone(), plan_ctx, graph_schema, collected_graph_joins);
    //             group_by.rebuild_or_clone(child_tf, logical_plan.clone())
    //         },
    //         LogicalPlan::OrderBy(order_by) => {
    //             let child_tf = self.collect_graph_joins(order_by.input.clone(), plan_ctx, graph_schema, collected_graph_joins);
    //             order_by.rebuild_or_clone(child_tf, logical_plan.clone())
    //         },
    //         LogicalPlan::Skip(skip) => {
    //             let child_tf = self.collect_graph_joins(skip.input.clone(), plan_ctx, graph_schema, collected_graph_joins);
    //             skip.rebuild_or_clone(child_tf, logical_plan.clone())
    //         },
    //         LogicalPlan::Limit(limit) => {
    //             let child_tf = self.collect_graph_joins(limit.input.clone(), plan_ctx, graph_schema, collected_graph_joins);
    //             limit.rebuild_or_clone(child_tf, logical_plan.clone())
    //         },
    //     }
    // }

    fn infer_graph_join_2(&self, graph_rel: &GraphRel, plan_ctx: &mut PlanCtx, graph_schema: &GraphSchema, collected_graph_joins: &mut Vec<Join>, joined_entities: &mut HashSet<String>) {
        // get required information 
        let left_alias = &graph_rel.left_connection.clone().unwrap();
        let rel_alias = &graph_rel.alias;
        let right_alias = &graph_rel.right_connection.clone().unwrap();

        println!("left_alias - {} rel_alias - {} right_alias - {}", left_alias, rel_alias, right_alias);

        let left_ctx = plan_ctx.alias_table_ctx_map.get(left_alias).unwrap();
        let rel_ctx = plan_ctx.alias_table_ctx_map.get(rel_alias).unwrap();
        let right_ctx = plan_ctx.alias_table_ctx_map.get(right_alias).unwrap();

        let left_label = left_ctx.label.clone().unwrap();
        let rel_label = rel_ctx.label.clone().unwrap();
        let original_rel_label = rel_label.replace("_incoming", "").replace("_outgoing", "");
        let right_label = right_ctx.label.clone().unwrap();

        let left_schema = graph_schema.nodes.get(&left_label).unwrap();
        let rel_schema = graph_schema.relationships.get(&original_rel_label).unwrap(); //.ok_or(AnalyzerError::NoRelationSchemaFound)?;
        let right_schema = graph_schema.nodes.get(&right_label).unwrap();

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

        if rel_ctx.use_edge_list {

        } else {

        }

    }

    fn infer_graph_join_1(&self, graph_rel: &GraphRel, plan_ctx: &mut PlanCtx, graph_schema: &GraphSchema, collected_graph_joins: &mut Vec<Join>, joined_entities: &mut HashSet<String>) {
        // get required information 
        let left_alias = &graph_rel.left_connection.clone().unwrap();
        let rel_alias = &graph_rel.alias;
        let right_alias = &graph_rel.right_connection.clone().unwrap();


        let left_ctx = plan_ctx.alias_table_ctx_map.get(left_alias).unwrap();
        let rel_ctx = plan_ctx.alias_table_ctx_map.get(rel_alias).unwrap();
        let right_ctx = plan_ctx.alias_table_ctx_map.get(right_alias).unwrap();

        let left_label = left_ctx.label.clone().unwrap();
        let rel_label = rel_ctx.label.clone().unwrap();
        let original_rel_label = rel_label.replace("_incoming", "").replace("_outgoing", "");
        let right_label = right_ctx.label.clone().unwrap();

        let left_schema = graph_schema.nodes.get(&left_label).unwrap();
        let rel_schema = graph_schema.relationships.get(&original_rel_label).unwrap(); //.ok_or(AnalyzerError::NoRelationSchemaFound)?;
        let right_schema = graph_schema.nodes.get(&right_label).unwrap();

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


        if rel_ctx.use_edge_list {
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
                                    PlanExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(rel_alias.to_string()), column: Column("from_id".to_string()) }),
                                    PlanExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(right_alias.to_string()), column: Column(right_node_id_column.clone()) })
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
                                    PlanExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(left_alias.to_string()), column: Column(left_node_id_column.clone()) }),
                                    PlanExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(rel_alias.to_string()), column: Column("to_id".to_string()) })
                                ]
                            }
                        ],
                    };

                    if is_standalone_rel {
                        let rel_to_right_graph_join_keys = 
                            OperatorApplication{
                                operator: Operator::Equal,
                                operands: vec![
                                    PlanExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(rel_alias.to_string()), column: Column("to_id".to_string()) }),
                                    PlanExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(left_alias.to_string()), column: Column(left_node_id_column) }),
                                ]
                            };
                        rel_graph_join.joining_on.push(rel_to_right_graph_join_keys);
    
                        collected_graph_joins.push(rel_graph_join);
                        joined_entities.insert(rel_alias.to_string());
                        // in this case we will only join relation so early return without pushing the other joins
                        return;
                    }
    
                    // push the relation first
                    collected_graph_joins.push(rel_graph_join);
                    joined_entities.insert(rel_alias.to_string());
    
                    joined_entities.insert(left_alias.to_string());
                    collected_graph_joins.push(left_graph_join);


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
                                    PlanExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(rel_alias.to_string()), column: Column("to_id".to_string()) }),
                                    PlanExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(left_alias.to_string()), column: Column(left_node_id_column.clone()) })
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
                                    PlanExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(right_alias.to_string()), column: Column(right_node_id_column.clone()) }),
                                    PlanExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(rel_alias.to_string()), column: Column("from_id".to_string()) })
                                ]
                            }
                        ],
                    };

                    if is_standalone_rel {
                        let rel_to_right_graph_join_keys = 
                            OperatorApplication{
                                operator: Operator::Equal,
                                operands: vec![
                                    PlanExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(rel_alias.to_string()), column: Column("from_id".to_string()) }),
                                    PlanExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(right_alias.to_string()), column: Column(right_node_id_column) }),
                                ]
                            };
                        rel_graph_join.joining_on.push(rel_to_right_graph_join_keys);

                        collected_graph_joins.push(rel_graph_join);
                        joined_entities.insert(rel_alias.to_string());
                        // in this case we will only join relation so early return without pushing the other joins
                        return;
                    }

                    // push the relation first
                    collected_graph_joins.push(rel_graph_join);
                    joined_entities.insert(rel_alias.to_string());

                    joined_entities.insert(right_alias.to_string());
                    collected_graph_joins.push(right_graph_join);
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
                                    PlanExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(rel_alias.to_string()), column: Column("to_id".to_string()) }),
                                    PlanExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(right_alias.to_string()), column: Column(right_node_id_column.clone()) })
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
                                    PlanExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(left_alias.to_string()), column: Column(left_node_id_column.clone()) }),
                                    PlanExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(rel_alias.to_string()), column: Column("from_id".to_string()) })
                                ]
                            }
                        ],
                    };

                    if is_standalone_rel {
                        let rel_to_right_graph_join_keys = 
                            OperatorApplication{
                                operator: Operator::Equal,
                                operands: vec![
                                    PlanExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(rel_alias.to_string()), column: Column("from_id".to_string()) }),
                                    PlanExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(left_alias.to_string()), column: Column(left_node_id_column) }),
                                ]
                            };
                        rel_graph_join.joining_on.push(rel_to_right_graph_join_keys);
    
                        collected_graph_joins.push(rel_graph_join);
                        joined_entities.insert(rel_alias.to_string());
                        // in this case we will only join relation so early return without pushing the other joins
                        return;
                    }
    
                    // push the relation first
                    collected_graph_joins.push(rel_graph_join);
                    joined_entities.insert(rel_alias.to_string());
    
                    joined_entities.insert(left_alias.to_string());
                    collected_graph_joins.push(left_graph_join);
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
                                    PlanExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(rel_alias.to_string()), column: Column("from_id".to_string()) }),
                                    PlanExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(left_alias.to_string()), column: Column(left_node_id_column.clone()) })
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
                                    PlanExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(right_alias.to_string()), column: Column(right_node_id_column.clone()) }),
                                    PlanExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(rel_alias.to_string()), column: Column("to_id".to_string()) })
                                ]
                            }
                        ],
                    };

                    if is_standalone_rel {
                        let rel_to_right_graph_join_keys = 
                            OperatorApplication{
                                operator: Operator::Equal,
                                operands: vec![
                                    PlanExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(rel_alias.to_string()), column: Column("to_id".to_string()) }),
                                    PlanExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(right_alias.to_string()), column: Column(right_node_id_column) }),
                                ]
                            };
                        rel_graph_join.joining_on.push(rel_to_right_graph_join_keys);

                        collected_graph_joins.push(rel_graph_join);
                        joined_entities.insert(rel_alias.to_string());
                        // in this case we will only join relation so early return without pushing the other joins
                        return;
                    }

                    // push the relation first
                    collected_graph_joins.push(rel_graph_join);
                    joined_entities.insert(rel_alias.to_string());

                    joined_entities.insert(right_alias.to_string());
                    collected_graph_joins.push(right_graph_join);
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
                                PlanExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(rel_alias.to_string()), column: Column("from_id".to_string()) }),
                                PlanExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(right_alias.to_string()), column: Column(right_node_id_column.clone()) })
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
                                PlanExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(left_alias.to_string()), column: Column(left_node_id_column.clone()) }),
                                PlanExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(rel_alias.to_string()), column: Column("to_id".to_string()) })
                            ]
                        }
                    ],
                };

                if is_standalone_rel {
                    let rel_to_right_graph_join_keys = 
                        OperatorApplication{
                            operator: Operator::Equal,
                            operands: vec![
                                PlanExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(rel_alias.to_string()), column: Column("to_id".to_string()) }),
                                PlanExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(left_alias.to_string()), column: Column(left_node_id_column) }),
                            ]
                        };
                    rel_graph_join.joining_on.push(rel_to_right_graph_join_keys);

                    collected_graph_joins.push(rel_graph_join);
                    joined_entities.insert(rel_alias.to_string());
                    // in this case we will only join relation so early return without pushing the other joins
                    return;
                }

                // push the relation first
                collected_graph_joins.push(rel_graph_join);
                joined_entities.insert(rel_alias.to_string());

                joined_entities.insert(left_alias.to_string());
                collected_graph_joins.push(left_graph_join);

    
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
                                PlanExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(rel_alias.to_string()), column: Column("to_id".to_string()) }),
                                PlanExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(left_alias.to_string()), column: Column(left_node_id_column.clone()) })
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
                                PlanExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(right_alias.to_string()), column: Column(right_node_id_column.clone()) }),
                                PlanExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(rel_alias.to_string()), column: Column("from_id".to_string()) })
                            ]
                        }
                    ],
                };

                if is_standalone_rel {
                    let rel_to_right_graph_join_keys = 
                        OperatorApplication{
                            operator: Operator::Equal,
                            operands: vec![
                                PlanExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(rel_alias.to_string()), column: Column("from_id".to_string()) }),
                                PlanExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(right_alias.to_string()), column: Column(right_node_id_column) }),
                            ]
                        };
                    rel_graph_join.joining_on.push(rel_to_right_graph_join_keys);

                    collected_graph_joins.push(rel_graph_join);
                    joined_entities.insert(rel_alias.to_string());
                    // in this case we will only join relation so early return without pushing the other joins
                    return;
                }

                // push the relation first
                collected_graph_joins.push(rel_graph_join);
                joined_entities.insert(rel_alias.to_string());

                joined_entities.insert(right_alias.to_string());
                collected_graph_joins.push(right_graph_join);

            }
             
        }



    }

    fn infer_graph_join(&self, graph_rel: &GraphRel, plan_ctx: &mut PlanCtx, graph_schema: &GraphSchema, collected_graph_joins: &mut Vec<Join>, joined_entities: &mut HashSet<String>) {
        println!("\n joined_entities {:?}", joined_entities);
        // get required information 
        let left_alias = &graph_rel.left_connection.clone().unwrap();
        let rel_alias = &graph_rel.alias;
        let right_alias = &graph_rel.right_connection.clone().unwrap();

        println!("left_alias - {} rel_alias - {} right_alias - {}", left_alias, rel_alias, right_alias);

        let left_ctx = plan_ctx.alias_table_ctx_map.get(left_alias).unwrap();
        let rel_ctx = plan_ctx.alias_table_ctx_map.get(rel_alias).unwrap();
        let right_ctx = plan_ctx.alias_table_ctx_map.get(right_alias).unwrap();

        let left_label = left_ctx.label.clone().unwrap();
        let rel_label = rel_ctx.label.clone().unwrap();
        let original_rel_label = rel_label.replace("_incoming", "").replace("_outgoing", "");
        let right_label = right_ctx.label.clone().unwrap();

        let left_schema = graph_schema.nodes.get(&left_label).unwrap();
        let rel_schema = graph_schema.relationships.get(&original_rel_label).unwrap(); //.ok_or(AnalyzerError::NoRelationSchemaFound)?;
        let right_schema = graph_schema.nodes.get(&right_label).unwrap();

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

        if rel_ctx.use_edge_list && rel_schema.to_node == right_schema.table_name{
            let mut rel_graph_join = Join{
                table_name: rel_cte_name,
                table_alias: rel_alias.to_string(),
                joining_on: vec![
                    OperatorApplication{
                        operator: Operator::Equal,
                        operands: vec![
                            PlanExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(rel_alias.to_string()), column: Column("from_id".to_string())}),
                            PlanExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(left_alias.to_string()), column: Column(left_node_id_column.clone()) })
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
                            PlanExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(left_alias.to_string()), column: Column(left_node_id_column.clone()) }),
                            PlanExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(rel_alias.to_string()), column: Column("from_id".to_string()) })
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
                            PlanExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(right_alias.to_string()), column: Column(right_node_id_column.clone()) }),
                            PlanExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(rel_alias.to_string()), column: Column("to_id".to_string()) })
                        ]
                    }
                ],
            };

            if is_standalone_rel {
                let rel_to_right_graph_join_keys = 
                    OperatorApplication{
                        operator: Operator::Equal,
                        operands: vec![
                            PlanExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(rel_alias.to_string()), column: Column("to_id".to_string()) }),
                            PlanExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(right_alias.to_string()), column: Column(right_node_id_column) }),
                        ]
                    };
                rel_graph_join.joining_on.push(rel_to_right_graph_join_keys);
                // plan_ctx.graph_joins.push(rel_graph_join);
                collected_graph_joins.push(rel_graph_join);
                joined_entities.insert(rel_alias.to_string());
                // in this case we will only join relation so early return without pushing the right_graph_join
                return;
            }
            


            if !joined_entities.contains(right_alias) {

                collected_graph_joins.push(rel_graph_join);
                joined_entities.insert(rel_alias.to_string());

                joined_entities.insert(right_alias.to_string());
                collected_graph_joins.push(right_graph_join);

            } else if !joined_entities.contains(left_alias){

                joined_entities.insert(left_alias.to_string());
                collected_graph_joins.push(left_graph_join);

                collected_graph_joins.push(rel_graph_join);
                joined_entities.insert(rel_alias.to_string());
            }
            // plan_ctx.graph_joins.push(rel_graph_join);
            // plan_ctx.graph_joins.push(right_graph_join);
        } else {
            let mut rel_graph_join = Join{
                table_name: rel_cte_name,
                table_alias: rel_alias.to_string(),
                joining_on: vec![
                    OperatorApplication{
                        operator: Operator::Equal,
                        operands: vec![
                            PlanExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(rel_alias.to_string()), column: Column("to_id".to_string()) }),
                            PlanExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(left_alias.to_string()), column: Column(left_node_id_column.clone()) })
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
                            PlanExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(left_alias.to_string()), column: Column(left_node_id_column.clone()) }),
                            PlanExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(rel_alias.to_string()), column: Column("to_id".to_string()) })
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
                            PlanExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(right_alias.to_string()), column: Column(right_node_id_column.clone()) }),
                            PlanExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(rel_alias.to_string()), column: Column("from_id".to_string()) })
                        ]
                    }
                ],
            };

            if is_standalone_rel {
                let rel_to_right_graph_join_keys = OperatorApplication{
                    operator: Operator::Equal,
                    operands: vec![
                        PlanExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(rel_alias.to_string()), column: Column("from_id".to_string()) }),
                        PlanExpr::PropertyAccessExp(PropertyAccess{ table_alias: TableAlias(right_alias.to_string()), column: Column(right_node_id_column) }),
                    ]
                };
                rel_graph_join.joining_on.push(rel_to_right_graph_join_keys);
                collected_graph_joins.push(rel_graph_join);
                joined_entities.insert(rel_alias.to_string());
                // in this case we will only join relation so early return without pushing the right_graph_join
                return;
            }

            if !joined_entities.contains(right_alias) {

                collected_graph_joins.push(rel_graph_join);
                joined_entities.insert(rel_alias.to_string());

                joined_entities.insert(right_alias.to_string());
                collected_graph_joins.push(right_graph_join);

            } else if !joined_entities.contains(left_alias){

                joined_entities.insert(left_alias.to_string());
                collected_graph_joins.push(left_graph_join);

                collected_graph_joins.push(rel_graph_join);
                joined_entities.insert(rel_alias.to_string());
            }
        }

    }

    // pub fn infer_graph_join_old(&self, graph_rel: &GraphRel, plan_ctx: &mut PlanCtx, graph_schema: &GraphSchema){
    //     // get required information 
    //     let left_alias = &graph_rel.left_connection.clone().unwrap();
    //     let rel_alias = &graph_rel.alias;
    //     let right_alias = &graph_rel.right_connection.clone().unwrap();


    //     let left_ctx = plan_ctx.alias_table_ctx_map.get(left_alias).unwrap();
    //     let rel_ctx = plan_ctx.alias_table_ctx_map.get(rel_alias).unwrap();
    //     let right_ctx = plan_ctx.alias_table_ctx_map.get(right_alias).unwrap();

    //     let left_label = left_ctx.label.clone().unwrap();
    //     let mut rel_label = rel_ctx.label.clone().unwrap();
    //     rel_label = rel_label.replace("_incoming", "").replace("_outgoing", "");
    //     let right_label = right_ctx.label.clone().unwrap();


    //     let left_schema = graph_schema.nodes.get(&left_label).unwrap();
    //     let rel_schema = graph_schema.relationships.get(&rel_label).unwrap(); //.ok_or(AnalyzerError::NoRelationSchemaFound)?;
    //     let right_schema = graph_schema.nodes.get(&right_label).unwrap();

    //     let rel_cte_name = format!("{}_{}", rel_label, rel_alias);
    //     let right_cte_name = format!("{}_{}", right_label, right_alias);

    //     let left_node_id_column = left_schema.node_id.column.clone();
    //     let right_node_id_column = right_schema.node_id.column.clone();   

    //     // Check for standalone relationship join. 
    //     // e.g. MATCH (a)-[f1:Follows]->(b)-[f2:Follows]->(c), (a)-[f3:Follows]->(c)
    //     // In the duplicate scan removing pass, we remove the already scanned nodes. We do this from bottom to up.
    //     // So there could be a graph_rel who has LogicalPlan::Empty as left. In such case just join the relationship but on both nodes columns.
    //     // In case of f3, both of its nodes a and b are already joined. So just join f3 on both a and b's joining keys.
    //     let is_standalone_rel = matches!(graph_rel.left.as_ref(), LogicalPlan::Empty);

    //     if rel_ctx.use_edge_list {
    //         // when using edge list, we need to check which node joins to "from_id" and which node joins to "to_id"
    //         if rel_schema.from_node == right_schema.table_name {

    //             let mut rel_graph_join = GraphJoin{
    //                 table_name: rel_cte_name,
    //                 table_alias: rel_alias.to_string(),
    //                 joining_on: vec![
    //                     PropertyAccess{ table_alias: TableAlias(rel_alias.to_string()), column: Column("to_id".to_string()) },
    //                     PropertyAccess{ table_alias: TableAlias(left_alias.to_string()), column: Column(left_node_id_column) }
    //                 ],
    //             };

    //             let right_graph_join = GraphJoin {
    //                 table_name: right_cte_name,
    //                 table_alias: right_alias.to_string(),
    //                 joining_on: vec![
    //                     PropertyAccess{ table_alias: TableAlias(right_alias.to_string()), column: Column(right_node_id_column.clone()) },
    //                     PropertyAccess{ table_alias: TableAlias(rel_alias.to_string()), column: Column("from_id".to_string()) }
    //                 ],
    //             };

    //             if is_standalone_rel {
    //                 let mut rel_to_right_graph_join_keys = vec![
    //                     PropertyAccess{ table_alias: TableAlias(rel_alias.to_string()), column: Column("from_id".to_string()) },
    //                     PropertyAccess{ table_alias: TableAlias(right_alias.to_string()), column: Column(right_node_id_column) },
    //                 ]; 
    //                 rel_graph_join.joining_on.append(&mut rel_to_right_graph_join_keys);
    //                 plan_ctx.graph_joins.push(rel_graph_join);
    //                 // in this case we will only join relation so early return without pushing the right_graph_join
    //                 return;
    //             }

    //             plan_ctx.graph_joins.push(rel_graph_join);
    //             plan_ctx.graph_joins.push(right_graph_join);
    //             return;
    //         } else{
    //             let mut rel_graph_join = GraphJoin{
    //                 table_name: rel_cte_name,
    //                 table_alias: rel_alias.to_string(),
    //                 joining_on: vec![
    //                     PropertyAccess{ table_alias: TableAlias(rel_alias.to_string()), column: Column("from_id".to_string()) },
    //                     PropertyAccess{ table_alias: TableAlias(left_alias.to_string()), column: Column(left_node_id_column) }
    //                 ],
    //             };

    //             let right_graph_join = GraphJoin {
    //                 table_name: right_cte_name,
    //                 table_alias: right_alias.to_string(),
    //                 joining_on: vec![
    //                     PropertyAccess{ table_alias: TableAlias(right_alias.to_string()), column: Column(right_node_id_column.clone()) },
    //                     PropertyAccess{ table_alias: TableAlias(rel_alias.to_string()), column: Column("to_id".to_string()) }
    //                 ],
    //             };

    //             if is_standalone_rel {
    //                 let mut rel_to_right_graph_join_keys = vec![
    //                     PropertyAccess{ table_alias: TableAlias(rel_alias.to_string()), column: Column("to_id".to_string()) },
    //                     PropertyAccess{ table_alias: TableAlias(right_alias.to_string()), column: Column(right_node_id_column) },
    //                 ]; 
    //                 rel_graph_join.joining_on.append(&mut rel_to_right_graph_join_keys);
    //                 plan_ctx.graph_joins.push(rel_graph_join);
    //                 // in this case we will only join relation so early return without pushing the right_graph_join
    //                 return;
    //             }

    //             plan_ctx.graph_joins.push(rel_graph_join);
    //             plan_ctx.graph_joins.push(right_graph_join);
    //         }
    //     } else {

    //         let mut rel_graph_join = GraphJoin{
    //             table_name: rel_cte_name,
    //             table_alias: rel_alias.to_string(),
    //             joining_on: vec![
    //                 PropertyAccess{ table_alias: TableAlias(rel_alias.to_string()), column: Column("to_id".to_string()) },
    //                 PropertyAccess{ table_alias: TableAlias(left_alias.to_string()), column: Column(left_node_id_column) }
    //             ],
    //         };

    //         let right_graph_join = GraphJoin {
    //             table_name: right_cte_name,
    //             table_alias: right_alias.to_string(),
    //             joining_on: vec![
    //                 PropertyAccess{ table_alias: TableAlias(right_alias.to_string()), column: Column(right_node_id_column.clone()) },
    //                 PropertyAccess{ table_alias: TableAlias(rel_alias.to_string()), column: Column("from_id".to_string()) }
    //             ],
    //         };

    //         if is_standalone_rel {
    //             let mut rel_to_right_graph_join_keys = vec![
    //                 PropertyAccess{ table_alias: TableAlias(rel_alias.to_string()), column: Column("from_id".to_string()) },
    //                 PropertyAccess{ table_alias: TableAlias(right_alias.to_string()), column: Column(right_node_id_column) },
    //             ]; 
    //             rel_graph_join.joining_on.append(&mut rel_to_right_graph_join_keys);
    //             plan_ctx.graph_joins.push(rel_graph_join);
    //             // in this case we will only join relation so early return without pushing the right_graph_join
    //             return;
    //         }

    //         plan_ctx.graph_joins.push(rel_graph_join);
    //         plan_ctx.graph_joins.push(right_graph_join);

    //     }

        

    // }


    // fn get_cte_name(&self, logical_plan: LogicalPlan) -> Result<String, AnalyzerError>{
    //     match logical_plan {
    //         LogicalPlan::Cte(cte) => {
    //             Ok(cte.name.clone())
    //         },
    //         _ => Err(AnalyzerError::NonCTEPlanFound)
    //     }
    // }
}
