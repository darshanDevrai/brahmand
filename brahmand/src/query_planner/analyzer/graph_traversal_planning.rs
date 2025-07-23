use std::sync::Arc;

use crate::{query_engine::types::{GraphSchema, RelationshipSchema}, query_planner::{analyzer::{analyzer_pass::{AnalyzerPass, AnalyzerResult}, errors::AnalyzerError}, logical_expr::logical_expr::{Column, ColumnAlias, Direction, InSubquery, LogicalExpr, PropertyAccess}, logical_plan::logical_plan::{Cte, GraphRel, LogicalPlan, Projection, ProjectionItem, Scan}, plan_ctx::plan_ctx::PlanCtx, transformed::Transformed}};









pub struct GraphTRaversalPlanning;


impl AnalyzerPass for GraphTRaversalPlanning {
    fn analyze_with_graph_schema(&self, logical_plan: Arc<LogicalPlan>, plan_ctx: &mut PlanCtx, graph_schema: &GraphSchema) -> AnalyzerResult<Transformed<Arc<LogicalPlan>>> {
        match logical_plan.as_ref() {
            LogicalPlan::Projection(projection) => {
                let child_tf = self.analyze_with_graph_schema(projection.input.clone(), plan_ctx, graph_schema)?;
                Ok(projection.rebuild_or_clone(child_tf, logical_plan.clone()))
            },
            LogicalPlan::GraphNode(graph_node) => {
                let child_tf = self.analyze_with_graph_schema(graph_node.input.clone(), plan_ctx, graph_schema)?;
                Ok(graph_node.rebuild_or_clone(child_tf, logical_plan.clone()))
            },
            LogicalPlan::GraphRel(graph_rel) => {

                if !matches!(graph_rel.right.as_ref(), LogicalPlan::GraphRel(_)) {
                    let (new_graph_rel, ctxs_to_update) = self.infer_anchor_traversal(graph_rel, plan_ctx, graph_schema)?;

                    for mut ctx in ctxs_to_update.into_iter() {
                        let table_ctx = plan_ctx.get_mut_table_ctx(&ctx.alias)?;
                        table_ctx.set_label(Some(ctx.label));
                        // table_ctx.projection_items.append(&mut ctx.projections);
                        if let Some(plan_expr) = ctx.insubquery {
                            table_ctx.insert_filter(plan_expr);
                        } 
                        table_ctx.append_projection(&mut ctx.projections);
                    }

                    return  Ok(Transformed::Yes(Arc::new(LogicalPlan::GraphRel(new_graph_rel))));

                } else {

                    let right_tf = self.analyze_with_graph_schema(graph_rel.right.clone(), plan_ctx, graph_schema)?;

                    let updated_graph_rel = GraphRel {
                        right: right_tf.get_plan(),
                        ..graph_rel.clone()
                    };
                    let (new_graph_rel, ctxs_to_update) = self.infer_intermediate_traversal(&updated_graph_rel, plan_ctx, graph_schema)?;

                    for mut ctx in ctxs_to_update.into_iter() {
                        let table_ctx = plan_ctx.get_mut_table_ctx(&ctx.alias)?;
                        table_ctx.set_label(Some(ctx.label));
                        if let Some(plan_expr) = ctx.insubquery {
                            table_ctx.insert_filter(plan_expr);
                        } 
                        table_ctx.append_projection(&mut ctx.projections);
                    }

                    return  Ok(Transformed::Yes(Arc::new(LogicalPlan::GraphRel(new_graph_rel))));
                }
            },
            LogicalPlan::Cte(cte   ) => {
                let child_tf = self.analyze_with_graph_schema( cte.input.clone(), plan_ctx, graph_schema)?;
                Ok(cte.rebuild_or_clone(child_tf, logical_plan.clone()))
            },
            LogicalPlan::Scan(_) => {
                Ok(Transformed::No(logical_plan.clone()))
            },
            LogicalPlan::Empty => Ok(Transformed::No(logical_plan.clone())),
            LogicalPlan::GraphJoins(graph_joins) => {
                let child_tf = self.analyze_with_graph_schema(graph_joins.input.clone(), plan_ctx, graph_schema)?;
                Ok(graph_joins.rebuild_or_clone(child_tf, logical_plan.clone()))
            },
            LogicalPlan::Filter(filter) => {
                let child_tf = self.analyze_with_graph_schema(filter.input.clone(), plan_ctx, graph_schema)?;
                Ok(filter.rebuild_or_clone(child_tf, logical_plan.clone()))
            },
            LogicalPlan::GroupBy(group_by   ) => {
                let child_tf = self.analyze_with_graph_schema(group_by.input.clone(), plan_ctx, graph_schema)?;
                Ok(group_by.rebuild_or_clone(child_tf, logical_plan.clone()))
            },
            LogicalPlan::OrderBy(order_by) => {
                let child_tf = self.analyze_with_graph_schema(order_by.input.clone(), plan_ctx, graph_schema)?;
                Ok(order_by.rebuild_or_clone(child_tf, logical_plan.clone()))
            },
            LogicalPlan::Skip(skip) => {
                let child_tf = self.analyze_with_graph_schema(skip.input.clone(), plan_ctx, graph_schema)?;
                Ok(skip.rebuild_or_clone(child_tf, logical_plan.clone()))
            },
            LogicalPlan::Limit(limit) => {
                let child_tf = self.analyze_with_graph_schema(limit.input.clone(), plan_ctx, graph_schema)?;
                Ok(limit.rebuild_or_clone(child_tf, logical_plan.clone()))
            },
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct CtxToUpdate {
    alias: String,
    label: String,
    projections: Vec<ProjectionItem>,
    insubquery: Option<LogicalExpr>,
}

impl GraphTRaversalPlanning {
    pub fn new() -> Self {
        GraphTRaversalPlanning
    }

    pub fn infer_anchor_traversal(&self, graph_rel: &GraphRel, plan_ctx: &mut PlanCtx, graph_schema: &GraphSchema) -> AnalyzerResult<(GraphRel, Vec<CtxToUpdate>)> {
        // get required information 
        let left_alias = &graph_rel.left_connection.clone().unwrap();
        let rel_alias = &graph_rel.alias;
        let right_alias = &graph_rel.right_connection.clone().unwrap();


        let left_ctx = plan_ctx.get_node_table_ctx(left_alias)?;
        let rel_ctx = plan_ctx.get_rel_table_ctx(rel_alias)?;
        let right_ctx = plan_ctx.get_node_table_ctx(right_alias)?;

        let left_label = left_ctx.get_label_str()?;
        let rel_label = rel_ctx.get_label_str()?;
        let right_label = right_ctx.get_label_str()?;


        let left_schema = graph_schema.nodes.get(&left_label).unwrap();
        let rel_schema = graph_schema.relationships.get(&rel_label).unwrap(); //.ok_or(AnalyzerError::NoRelationSchemaFound)?;
        let right_schema = graph_schema.nodes.get(&right_label).unwrap();

        let left_node_id_column = left_schema.node_id.column.clone();
        let right_node_id_column = right_schema.node_id.column.clone();

        let mut ctxs_to_update: Vec<CtxToUpdate> = vec![];


        let left_cte_name = format!("{}_{}",left_label, left_alias);
        let right_cte_name = format!("{}_{}", right_label, right_alias);

        let star_found = right_ctx.get_projections().iter().any(|item| item.expression == LogicalExpr::Star);
        let node_id_found = right_ctx.get_projections().iter().any(|item| {
            match &item.expression {
                LogicalExpr::Column(Column(col)) => col == &right_node_id_column,
                LogicalExpr::PropertyAccessExp(PropertyAccess { column, .. }) => column.0 == right_node_id_column,
                _ => false,
            }
        });
        let right_projections: Vec<ProjectionItem> = if !star_found && !node_id_found {
            let proj_input: Vec<(String, Option<ColumnAlias>)> = vec![(right_node_id_column.clone(), None)];
            self.build_projections(proj_input)
        } else {
            vec![]
        };
        

        let star_found = left_ctx.get_projections().iter().any(|item| item.expression == LogicalExpr::Star);
        let node_id_found = left_ctx.get_projections().iter().any(|item| {
            match &item.expression {
                LogicalExpr::Column(Column(col)) => col == &left_node_id_column,
                LogicalExpr::PropertyAccessExp(PropertyAccess { column, .. }) => column.0 == left_node_id_column,
                _ => false,
            }
        });
        let left_projections: Vec<ProjectionItem> = if !star_found && !node_id_found {
            let proj_input: Vec<(String, Option<ColumnAlias>)> = vec![(left_node_id_column.clone(), None)];
            self.build_projections(proj_input)
        } else {
            vec![]
        };

        if rel_ctx.should_use_edge_list() {
            let rel_cte_name = format!("{}_{}", rel_label, rel_alias);
            
            let star_found = rel_ctx.get_projections().iter().any(|item| item.expression == LogicalExpr::Star);
            let rel_proj_input: Vec<(String, Option<ColumnAlias>)> = if !star_found {
                vec![
                    (format!("from_{}", rel_schema.from_node), Some(ColumnAlias("from_id".to_string()))),
                    (format!("to_{}", rel_schema.to_node.clone()), Some(ColumnAlias("to_id".to_string())))
                ]
            } else { vec![] };

            let rel_projections:Vec<ProjectionItem> = self.build_projections(rel_proj_input);
            let rel_insubquery: LogicalExpr;
            let right_insubquery: LogicalExpr;
            let left_insubquery: LogicalExpr;
            // when using edge list, we need to check which node joins to "from_id" and which node joins to "to_id"
            if rel_schema.from_node == right_schema.table_name {
                rel_insubquery = self.build_insubquery("from_id".to_string(),
                right_cte_name.clone(),
                right_node_id_column.clone());

                right_insubquery = self.build_insubquery(right_node_id_column,
                    rel_cte_name.clone(),
                    "from_id".to_string());

                left_insubquery = self.build_insubquery(left_node_id_column,
                    rel_cte_name.clone(),
                    "to_id".to_string());
                
            }else{
                rel_insubquery = self.build_insubquery("to_id".to_string(),
                right_cte_name.clone(),
                right_node_id_column.clone());

                right_insubquery = self.build_insubquery(right_node_id_column,
                    rel_cte_name.clone(),
                    "to_id".to_string());

                left_insubquery = self.build_insubquery(left_node_id_column,
                    rel_cte_name.clone(),
                    "from_id".to_string());
            }

            if graph_rel.is_rel_anchor {
                let right_ctx_to_update = CtxToUpdate {
                    alias: right_alias.to_string(),
                    label: right_label,
                    projections: right_projections,
                    insubquery: Some(right_insubquery),
                };
                ctxs_to_update.push(right_ctx_to_update);

                let rel_ctx_to_update = CtxToUpdate {
                    alias: rel_alias.to_string(),
                    label: rel_label,
                    projections: rel_projections,
                    insubquery: None,
                };
                ctxs_to_update.push(rel_ctx_to_update);

                let left_ctx_to_update = CtxToUpdate {
                    alias: left_alias.to_string(),
                    label: left_label,
                    projections: left_projections,
                    insubquery: Some(left_insubquery),
                };
                ctxs_to_update.push(left_ctx_to_update);

                let new_graph_rel = GraphRel { 
                    left: Arc::new(LogicalPlan::Cte(Cte { input: graph_rel.left.clone(), name: left_cte_name })),
                    center: Arc::new(LogicalPlan::Cte(Cte { input: graph_rel.center.clone(), name: right_cte_name })),
                    right: Arc::new(LogicalPlan::Cte(Cte { input: graph_rel.right.clone(), name: rel_cte_name  })), 
                    ..graph_rel.clone()
                };
                return Ok((new_graph_rel, ctxs_to_update))

            }else{
                let right_ctx_to_update = CtxToUpdate {
                    alias: right_alias.to_string(),
                    label: right_label,
                    projections: right_projections,
                    insubquery: None,
                };
                ctxs_to_update.push(right_ctx_to_update);

                let rel_ctx_to_update = CtxToUpdate {
                    alias: rel_alias.to_string(),
                    label: rel_label,
                    projections: rel_projections,
                    insubquery: Some(rel_insubquery),
                };
                ctxs_to_update.push(rel_ctx_to_update);

                let left_ctx_to_update = CtxToUpdate {
                    alias: left_alias.to_string(),
                    label: left_label,
                    projections: left_projections,
                    insubquery: Some(left_insubquery),
                };
                ctxs_to_update.push(left_ctx_to_update);


                let new_graph_rel = GraphRel { 
                    left: Arc::new(LogicalPlan::Cte(Cte { input: graph_rel.left.clone(), name: left_cte_name })),
                    center: Arc::new(LogicalPlan::Cte(Cte { input: graph_rel.center.clone(), name: rel_cte_name })),
                    right: Arc::new(LogicalPlan::Cte(Cte { input: graph_rel.right.clone(), name: right_cte_name })), 
                    ..graph_rel.clone()
                };
    
                return Ok((new_graph_rel, ctxs_to_update))
            }


            
        } else {
           

            let new_rel_label = self.get_relationship_table_name(right_label.clone(), left_label.clone(), rel_label, graph_rel.direction.clone(), rel_schema)?;
            let rel_cte_name = format!("{}_{}", new_rel_label, rel_alias);

            let right_ctx_to_update = CtxToUpdate {
                alias: right_alias.to_string(),
                label: right_label,
                projections: right_projections,
                insubquery: None,
            };
            ctxs_to_update.push(right_ctx_to_update);

            let rel_proj_input: Vec<(String, Option<ColumnAlias>)> = vec![
                ("from_id".to_string(), None),
                ("arrayJoin(bitmapToArray(to_id))".to_string(), Some(ColumnAlias("to_id".to_string())))
            ];
            let rel_projections = self.build_projections(rel_proj_input);
            let rel_insubquery = self.build_insubquery("from_id".to_string(),
                right_cte_name.clone(),
                right_node_id_column);
            let rel_ctx_to_update = CtxToUpdate {
                alias: rel_alias.to_string(),
                label: new_rel_label,
                projections: rel_projections,
                insubquery: Some(rel_insubquery),
            };
            ctxs_to_update.push(rel_ctx_to_update);
            

            let left_insubquery = self.build_insubquery(left_node_id_column,
                rel_cte_name.clone(),
                "to_id".to_string());
            let left_ctx_to_update = CtxToUpdate {
                alias: left_alias.to_string(),
                label: left_label,
                projections: left_projections,
                insubquery: Some(left_insubquery),
            };
            ctxs_to_update.push(left_ctx_to_update);

            let new_graph_rel = GraphRel { 
                left: Arc::new(LogicalPlan::Cte(Cte { input: graph_rel.left.clone(), name: left_cte_name })),
                center: Arc::new(LogicalPlan::Cte(Cte { input: graph_rel.center.clone(), name: rel_cte_name })),
                right: Arc::new(LogicalPlan::Cte(Cte { input: graph_rel.right.clone(), name: right_cte_name })), 
                ..graph_rel.clone()
            };

            Ok((new_graph_rel, ctxs_to_update))
        }

    }

    fn infer_intermediate_traversal(&self, graph_rel: &GraphRel, plan_ctx: &mut PlanCtx, graph_schema: &GraphSchema) -> AnalyzerResult<(GraphRel, Vec<CtxToUpdate>)> {
        // get required information 
        let left_alias = &graph_rel.left_connection.clone().unwrap();
        let rel_alias = &graph_rel.alias;
        let right_alias = &graph_rel.right_connection.clone().unwrap();


        let left_ctx = plan_ctx.get_node_table_ctx(left_alias)?;
        let rel_ctx = plan_ctx.get_rel_table_ctx(rel_alias)?;
        let right_ctx = plan_ctx.get_node_table_ctx(right_alias)?;

        let left_label = left_ctx.get_label_str()?;
        let rel_label = rel_ctx.get_label_str()?;
        let right_label = right_ctx.get_label_str()?;


        let left_schema = graph_schema.nodes.get(&left_label).unwrap();
        let rel_schema = graph_schema.relationships.get(&rel_label).unwrap(); //.ok_or(AnalyzerError::NoRelationSchemaFound)?;
        let right_schema = graph_schema.nodes.get(&right_label).unwrap();

        let left_node_id_column = left_schema.node_id.column.clone();
        let right_node_id_column = right_schema.node_id.column.clone();

        let mut ctxs_to_update: Vec<CtxToUpdate> = vec![];


        let left_cte_name = format!("{}_{}",left_label, left_alias);
        let right_cte_name = format!("{}_{}", right_label, right_alias);


        let star_found = left_ctx.get_projections().iter().any(|item| item.expression == LogicalExpr::Star);
        let node_id_found = left_ctx.get_projections().iter().any(|item| {
            match &item.expression {
                LogicalExpr::Column(Column(col)) => col == &left_node_id_column,
                LogicalExpr::PropertyAccessExp(PropertyAccess { column, .. }) => column.0 == left_node_id_column,
                _ => false,
            }
        });
        let left_projections: Vec<ProjectionItem> = if !star_found && !node_id_found {
            let proj_input: Vec<(String, Option<ColumnAlias>)> = vec![(left_node_id_column.clone(), None)];
            self.build_projections(proj_input)
        } else {
            vec![]
        };


        if rel_ctx.should_use_edge_list() {
            let rel_cte_name = format!("{}_{}", rel_label, rel_alias);
            
            let star_found = rel_ctx.get_projections().iter().any(|item| item.expression == LogicalExpr::Star);
            let rel_proj_input: Vec<(String, Option<ColumnAlias>)> = if !star_found {
                vec![
                    (format!("from_{}", rel_schema.from_node), Some(ColumnAlias("from_id".to_string()))),
                    (format!("to_{}", rel_schema.to_node.clone()), Some(ColumnAlias("to_id".to_string())))
                ]
            } else { vec![] };

            let rel_projections:Vec<ProjectionItem> = self.build_projections(rel_proj_input);
            let rel_insubquery: LogicalExpr;
            // let right_insubquery: LogicalExpr;
            let left_insubquery: LogicalExpr;
            // when using edge list, we need to check which node joins to "from_id" and which node joins to "to_id"
            if rel_schema.from_node == right_schema.table_name {
                rel_insubquery = self.build_insubquery("from_id".to_string(),
                right_cte_name.clone(),
                right_node_id_column.clone());

                left_insubquery = self.build_insubquery(left_node_id_column,
                    rel_cte_name.clone(),
                    "to_id".to_string());
                
            }else{
                rel_insubquery = self.build_insubquery("to_id".to_string(),
                right_cte_name.clone(),
                right_node_id_column.clone());

                left_insubquery = self.build_insubquery(left_node_id_column,
                    rel_cte_name.clone(),
                    "from_id".to_string());
            }

            let rel_ctx_to_update = CtxToUpdate {
                alias: rel_alias.to_string(),
                label: rel_cte_name.to_string(),
                projections: rel_projections,
                insubquery: Some(rel_insubquery),
            };
            ctxs_to_update.push(rel_ctx_to_update);

            let left_ctx_to_update = CtxToUpdate {
                alias: left_alias.to_string(),
                label: left_label,
                projections: left_projections,
                insubquery: Some(left_insubquery),
            };
            ctxs_to_update.push(left_ctx_to_update);


            let new_graph_rel = GraphRel { 
                left: Arc::new(LogicalPlan::Cte(Cte { input: graph_rel.left.clone(), name: left_cte_name })),
                center: Arc::new(LogicalPlan::Cte(Cte { input: graph_rel.center.clone(), name: rel_cte_name })),
                right: graph_rel.right.clone(), 
                ..graph_rel.clone()
            };

            return Ok((new_graph_rel, ctxs_to_update))
            
        } else{

            let new_rel_label = self.get_relationship_table_name(right_label.clone(), left_label.clone(), rel_label, graph_rel.direction.clone(), rel_schema)?;
            let rel_cte_name = format!("{}_{}", new_rel_label, rel_alias);


            let rel_proj_input: Vec<(String, Option<ColumnAlias>)> = vec![
                ("from_id".to_string(), None),
                ("arrayJoin(bitmapToArray(to_id))".to_string(), Some(ColumnAlias("to_id".to_string())))
            ];
            let rel_projections = self.build_projections(rel_proj_input);
            let rel_insubquery = self.build_insubquery("from_id".to_string(),
                right_cte_name.clone(),
                right_node_id_column);
            let rel_ctx_to_update = CtxToUpdate {
                alias: rel_alias.to_string(),
                label: new_rel_label,
                projections: rel_projections,
                insubquery: Some(rel_insubquery),
            };
            ctxs_to_update.push(rel_ctx_to_update);
            

            let left_insubquery = self.build_insubquery(left_node_id_column,
                rel_cte_name.clone(),
                "to_id".to_string());
            let left_ctx_to_update = CtxToUpdate {
                alias: left_alias.to_string(),
                label: left_label,
                projections: left_projections,
                insubquery: Some(left_insubquery),
            };
            ctxs_to_update.push(left_ctx_to_update);

            let new_graph_rel = GraphRel { 
                left: Arc::new(LogicalPlan::Cte(Cte { input: graph_rel.left.clone(), name: left_cte_name })),
                center: Arc::new(LogicalPlan::Cte(Cte { input: graph_rel.center.clone(), name: rel_cte_name })),
                right: graph_rel.right.clone(), 
                ..graph_rel.clone()
            };
            

            Ok((new_graph_rel, ctxs_to_update))
        }


    }


    fn build_projections(&self, items: Vec<(String, Option<ColumnAlias>)>) -> Vec<ProjectionItem> {
        items.into_iter().map(|(expr_str, alias)| {
            ProjectionItem{
                expression: LogicalExpr::Column(Column(expr_str)),
                col_alias: alias,
            }
        }).collect()
    }


    fn build_insubquery(&self, sub_in_exp: String, sub_plan_table: String, sub_plan_column: String) -> LogicalExpr{
        LogicalExpr::InSubquery(InSubquery{
            expr: Box::new(LogicalExpr::Column(Column(sub_in_exp))),
            subplan: self.get_subplan(sub_plan_table, sub_plan_column)
        })
    }

    fn get_subplan(&self, table_name: String, table_column: String) -> Arc<LogicalPlan>{
        Arc::new(LogicalPlan::Projection(Projection{
            input: Arc::new(LogicalPlan::Scan(Scan{
                table_alias: "".to_string(),
                table_name: Some(table_name),
            })),
            items: vec![ProjectionItem{ expression: LogicalExpr::Column(Column(table_column)), col_alias: None }],
        }))
    } 

    // We will get the correct table name for relation based start_node, end_node and relation schema stored in graph schema
    // Post --CREATED_BY---> User
    // CREATED_BY_outgoing = (Post, User)
    // CREATED_BY_incoming = (User, Post)
    // If from_node of schema == start_node -> outgoing with start_node = from_node and end_node = to_node
    // If from_node of schema == end_node  -> incoming with start_node = from_node and end_node = to_node
    fn get_relationship_table_name(
        &self,
        right_node_label: String,
        left_node_label: String,
        rel_label: String,
        direction: Direction,
        rel_schema: &RelationshipSchema,
    ) -> AnalyzerResult<String> {


        if right_node_label == left_node_label {
            if direction == Direction::Incoming {
                return Ok(format!("{}_incoming", rel_label));
            } else {
                return Ok(format!("{}_outgoing", rel_label));
            }
        }

        if rel_schema.from_node == right_node_label {
            return Ok(format!("{}_outgoing", rel_label));
        }

        if rel_schema.from_node == left_node_label {
            return Ok(format!("{}_incoming", rel_label));
        }

        Err(AnalyzerError::NoRelationSchemaFound)
    }
}