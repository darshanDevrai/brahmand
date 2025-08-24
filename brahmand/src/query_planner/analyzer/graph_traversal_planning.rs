use std::{collections::HashMap, sync::Arc};

use crate::{ graph_schema::graph_schema::{GraphSchema, NodeSchema, RelationshipSchema}, query_planner::{analyzer::{analyzer_pass::{AnalyzerPass, AnalyzerResult}, errors::{AnalyzerError, Pass}}, logical_expr::logical_expr::{Column, ColumnAlias, Direction, InSubquery, LogicalExpr, Operator, OperatorApplication, PropertyAccess}, logical_plan::{self, logical_plan::{Cte, Filter, GraphRel, LogicalPlan, Projection, ProjectionItem, Scan, Union}}, plan_ctx::plan_ctx::{PlanCtx, TableCtx}, transformed::Transformed}};









pub struct GraphTRaversalPlanning;


impl AnalyzerPass for GraphTRaversalPlanning {
    fn analyze_with_graph_schema(&self, logical_plan: Arc<LogicalPlan>, plan_ctx: &mut PlanCtx, graph_schema: &GraphSchema) -> AnalyzerResult<Transformed<Arc<LogicalPlan>>> {
        let transformed_plan = match logical_plan.as_ref() {
            LogicalPlan::Projection(projection) => {
                let child_tf = self.analyze_with_graph_schema(projection.input.clone(), plan_ctx, graph_schema)?;
                projection.rebuild_or_clone(child_tf, logical_plan.clone())
            },
            LogicalPlan::GraphNode(graph_node) => {
                let child_tf = self.analyze_with_graph_schema(graph_node.input.clone(), plan_ctx, graph_schema)?;
                graph_node.rebuild_or_clone(child_tf, logical_plan.clone())
            },
            LogicalPlan::GraphRel(graph_rel) => {

                if !matches!(graph_rel.right.as_ref(), LogicalPlan::GraphRel(_)) {
                    // let (new_graph_rel, ctxs_to_update) = self.infer_anchor_traversal(graph_rel, plan_ctx, graph_schema)?;
                    let (new_graph_rel, ctxs_to_update) = self.infer_traversal(graph_rel, plan_ctx, graph_schema, true)?;

                    for mut ctx in ctxs_to_update.into_iter() {

                        if let Some(table_ctx) = plan_ctx.get_mut_table_ctx_opt(&ctx.alias) {
                            table_ctx.set_label(Some(ctx.label));
                            // table_ctx.projection_items.append(&mut ctx.projections);
                            if let Some(plan_expr) = ctx.insubquery {
                                table_ctx.insert_filter(plan_expr);
                            } 
                            if ctx.override_projections {
                                table_ctx.set_projections(ctx.projections);
                            } else {
                                table_ctx.append_projection(&mut ctx.projections);
                            }
                        } else {
                            // add new table contexts
                            let mut new_table_ctx = TableCtx::build(ctx.alias.clone(), Some(ctx.label), vec![], ctx.is_rel, false);
                            if let Some(plan_expr) = ctx.insubquery {
                                new_table_ctx.insert_filter(plan_expr);
                            } 
                            new_table_ctx.set_projections(ctx.projections);

                            plan_ctx.insert_table_ctx(ctx.alias.clone(), new_table_ctx);
                        }
                    }

                    Transformed::Yes(Arc::new(LogicalPlan::GraphRel(new_graph_rel)))

                } else {

                    let right_tf = self.analyze_with_graph_schema(graph_rel.right.clone(), plan_ctx, graph_schema)?;

                    let updated_graph_rel = GraphRel {
                        right: right_tf.get_plan(),
                        ..graph_rel.clone()
                    };
                    // let (new_graph_rel, ctxs_to_update) = self.infer_intermediate_traversal(&updated_graph_rel, plan_ctx, graph_schema)?;
                    let (new_graph_rel, ctxs_to_update) = self.infer_traversal(&updated_graph_rel, plan_ctx, graph_schema, false)?;

                    for mut ctx in ctxs_to_update.into_iter() {
                        if let Some(table_ctx) = plan_ctx.get_mut_table_ctx_opt(&ctx.alias) {
                            table_ctx.set_label(Some(ctx.label));
                            // table_ctx.projection_items.append(&mut ctx.projections);
                            if let Some(plan_expr) = ctx.insubquery {
                                table_ctx.insert_filter(plan_expr);
                            } 
                            if ctx.override_projections {
                                table_ctx.set_projections(ctx.projections);
                            } else {
                                table_ctx.append_projection(&mut ctx.projections);
                            }
                        } else {
                            // add new table contexts
                            let mut new_table_ctx = TableCtx::build(ctx.alias.clone(), Some(ctx.label), vec![], ctx.is_rel, false);
                            if let Some(plan_expr) = ctx.insubquery {
                                new_table_ctx.insert_filter(plan_expr);
                            } 
                            new_table_ctx.set_projections(ctx.projections);

                            plan_ctx.insert_table_ctx(ctx.alias.clone(), new_table_ctx);
                        }
                    
                    }

                    Transformed::Yes(Arc::new(LogicalPlan::GraphRel(new_graph_rel)))
                }
            },
            LogicalPlan::Cte(cte   ) => {
                let child_tf = self.analyze_with_graph_schema( cte.input.clone(), plan_ctx, graph_schema)?;
                cte.rebuild_or_clone(child_tf, logical_plan.clone())
            },
            LogicalPlan::Scan(_) => {
                Transformed::No(logical_plan.clone())
            },
            LogicalPlan::Empty => Transformed::No(logical_plan.clone()),
            LogicalPlan::GraphJoins(graph_joins) => {
                let child_tf = self.analyze_with_graph_schema(graph_joins.input.clone(), plan_ctx, graph_schema)?;
                graph_joins.rebuild_or_clone(child_tf, logical_plan.clone())
            },
            LogicalPlan::Filter(filter) => {
                let child_tf = self.analyze_with_graph_schema(filter.input.clone(), plan_ctx, graph_schema)?;
                filter.rebuild_or_clone(child_tf, logical_plan.clone())
            },
            LogicalPlan::GroupBy(group_by   ) => {
                let child_tf = self.analyze_with_graph_schema(group_by.input.clone(), plan_ctx, graph_schema)?;
                group_by.rebuild_or_clone(child_tf, logical_plan.clone())
            },
            LogicalPlan::OrderBy(order_by) => {
                let child_tf = self.analyze_with_graph_schema(order_by.input.clone(), plan_ctx, graph_schema)?;
                order_by.rebuild_or_clone(child_tf, logical_plan.clone())
            },
            LogicalPlan::Skip(skip) => {
                let child_tf = self.analyze_with_graph_schema(skip.input.clone(), plan_ctx, graph_schema)?;
                skip.rebuild_or_clone(child_tf, logical_plan.clone())
            },
            LogicalPlan::Limit(limit) => {
                let child_tf = self.analyze_with_graph_schema(limit.input.clone(), plan_ctx, graph_schema)?;
                limit.rebuild_or_clone(child_tf, logical_plan.clone())
            },
            LogicalPlan::Union(union) => {
                let mut inputs_tf: Vec<Transformed<Arc<LogicalPlan>>> = vec![];
                for input_plan in union.inputs.iter() {
                    let child_tf = self.analyze_with_graph_schema(input_plan.clone(), plan_ctx, graph_schema)?; 
                    inputs_tf.push(child_tf);
                }
                union.rebuild_or_clone(inputs_tf, logical_plan.clone())
            },
        };
        Ok(transformed_plan)
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct CtxToUpdate {
    alias: String,
    label: String,
    projections: Vec<ProjectionItem>,
    insubquery: Option<LogicalExpr>,
    override_projections: bool,
    is_rel: bool
}

#[derive(Debug, Clone)]
pub struct GraphContext<'a> {
    left: GraphNodeContext<'a>,
    rel: GraphRelContext<'a>,
    right: GraphNodeContext<'a>
}

#[derive(Debug, Clone)]
pub struct GraphNodeContext<'a> {
    alias: &'a String,
    table_ctx: &'a TableCtx,
    label: String,
    schema: &'a NodeSchema,
    id_column: String,
    cte_name: String
}

#[derive(Debug, Clone)]
pub struct GraphRelContext<'a> {
    alias: &'a String,
    table_ctx: &'a TableCtx,
    label: String,
    schema: &'a RelationshipSchema,
    // id_column: String,
    // cte_name: String
}


impl GraphTRaversalPlanning {
    pub fn new() -> Self {
        GraphTRaversalPlanning
    }

    fn get_graph_context<'a>(&'a self,  graph_rel: &'a GraphRel, plan_ctx: &'a mut PlanCtx, graph_schema: &'a GraphSchema) -> AnalyzerResult<GraphContext<'a>> {
        // get required information 
        let left_alias = &graph_rel.left_connection;
        let rel_alias = &graph_rel.alias;
        let right_alias = &graph_rel.right_connection;


        let left_ctx = plan_ctx.get_node_table_ctx(left_alias).map_err(|e| AnalyzerError::PlanCtx { pass: Pass::GraphTraversalPlanning, source: e})?;
        let rel_ctx = plan_ctx.get_rel_table_ctx(rel_alias).map_err(|e| AnalyzerError::PlanCtx { pass: Pass::GraphTraversalPlanning, source: e})?;
        let right_ctx = plan_ctx.get_node_table_ctx(right_alias).map_err(|e| AnalyzerError::PlanCtx { pass: Pass::GraphTraversalPlanning, source: e})?;

        let left_label = left_ctx.get_label_str().map_err(|e| AnalyzerError::PlanCtx { pass: Pass::GraphTraversalPlanning, source: e})?;
        let rel_label = rel_ctx.get_label_str().map_err(|e| AnalyzerError::PlanCtx { pass: Pass::GraphTraversalPlanning, source: e})?;
        let right_label = right_ctx.get_label_str().map_err(|e| AnalyzerError::PlanCtx { pass: Pass::GraphTraversalPlanning, source: e})?;


        let left_schema = graph_schema.get_node_schema(&left_label).map_err(|e| AnalyzerError::GraphSchema { pass: Pass::GraphTraversalPlanning, source: e})?;
        let rel_schema = graph_schema.get_rel_schema(&rel_label).map_err(|e| AnalyzerError::GraphSchema { pass: Pass::GraphTraversalPlanning, source: e})?;
        let right_schema = graph_schema.get_node_schema(&right_label).map_err(|e| AnalyzerError::GraphSchema { pass: Pass::GraphTraversalPlanning, source: e})?;

        let left_node_id_column = left_schema.node_id.column.clone();
        let right_node_id_column = right_schema.node_id.column.clone();

        let left_cte_name = format!("{}_{}",left_label, left_alias);
        let right_cte_name = format!("{}_{}", right_label, right_alias);


        let graph_context = GraphContext {
            left: GraphNodeContext { alias: left_alias, table_ctx: left_ctx, label: left_label, schema: left_schema, id_column: left_node_id_column, cte_name: left_cte_name },
            rel: GraphRelContext { alias: rel_alias, table_ctx: rel_ctx, label: rel_label, schema: rel_schema},
            right: GraphNodeContext{ alias: right_alias, table_ctx: right_ctx, label: right_label, schema: right_schema, id_column: right_node_id_column, cte_name: right_cte_name },
        };

        Ok(graph_context)

    }

    fn infer_traversal(&self, graph_rel: &GraphRel, plan_ctx: &mut PlanCtx, graph_schema: &GraphSchema, is_anchor_traversal: bool) -> AnalyzerResult<(GraphRel, Vec<CtxToUpdate>)> {
        
        let graph_context = self.get_graph_context(graph_rel, plan_ctx, graph_schema)?;

        // left is traversed irrespective of anchor node or intermediate node
        let star_found = graph_context.left.table_ctx.get_projections().iter().any(|item| item.expression == LogicalExpr::Star);
        let node_id_found = graph_context.left.table_ctx.get_projections().iter().any(|item| {
            match &item.expression {
                LogicalExpr::Column(Column(col)) => col == &graph_context.left.id_column,
                LogicalExpr::PropertyAccessExp(PropertyAccess { column, .. }) => column.0 == graph_context.left.id_column,
                _ => false,
            }
        });
        let left_projections: Vec<ProjectionItem> = if !star_found && !node_id_found {
            let proj_input: Vec<(String, Option<ColumnAlias>)> = vec![(graph_context.left.id_column.clone(), None)];
            self.build_projections(proj_input)
        } else {
            vec![]
        };


        let star_found = graph_context.right.table_ctx.get_projections().iter().any(|item| item.expression == LogicalExpr::Star);
        let node_id_found = graph_context.right.table_ctx.get_projections().iter().any(|item| {
            match &item.expression {
                LogicalExpr::Column(Column(col)) => col == &graph_context.right.id_column,
                LogicalExpr::PropertyAccessExp(PropertyAccess { column, .. }) => column.0 == graph_context.right.id_column,
                _ => false,
            }
        });
        let right_projections: Vec<ProjectionItem> = if !star_found && !node_id_found {
            let proj_input: Vec<(String, Option<ColumnAlias>)> = vec![(graph_context.right.id_column.clone(), None)];
            self.build_projections(proj_input)
        } else {
            vec![]
        };

        if graph_context.rel.table_ctx.should_use_edge_list() {
            self.handle_edge_list_traversal(graph_rel,  graph_context, left_projections, right_projections, is_anchor_traversal)
        } else {
            self.handle_bitmap_traversal(graph_rel, graph_context, left_projections, right_projections, is_anchor_traversal)
        }


    }

    fn handle_edge_list_traversal(&self, graph_rel: &GraphRel, graph_context: GraphContext, left_projections: Vec<ProjectionItem>, right_projections: Vec<ProjectionItem>, is_anchor_traversal:bool) -> AnalyzerResult<(GraphRel, Vec<CtxToUpdate>)> {

        let mut ctxs_to_update: Vec<CtxToUpdate> = vec![];
    

        let rel_cte_name:String;
        let mut rel_ctxs_to_update: Vec<CtxToUpdate>;
        let rel_plan: Arc<LogicalPlan>;

        let right_insubquery: LogicalExpr;
        let left_insubquery: LogicalExpr;
        // when using edge list, we need to check which node joins to "from_id" and which node joins to "to_id"
        if graph_context.rel.schema.from_node == graph_context.right.schema.table_name {

            let (r_cte_name, r_plan, mut r_ctxs_to_update) = self.get_rel_ctx_for_edge_list(graph_rel, &graph_context,graph_context.right.cte_name.clone(), graph_context.right.id_column.clone(), graph_rel.is_rel_anchor);
            rel_cte_name = r_cte_name;
            rel_ctxs_to_update = r_ctxs_to_update;
            rel_plan = r_plan;

            right_insubquery = self.build_insubquery(graph_context.right.id_column.clone(),
                rel_cte_name.clone(),
                "from_id".to_string());

            left_insubquery = self.build_insubquery(graph_context.left.id_column,
                rel_cte_name.clone(),
                "to_id".to_string());
            
        }else{
            // rel_insubquery = self.build_insubquery("to_id".to_string(),
            // graph_context.right.cte_name.clone(),
            // graph_context.right.id_column.clone());

            let (r_cte_name, r_plan, mut r_ctxs_to_update) = self.get_rel_ctx_for_edge_list(graph_rel, &graph_context,graph_context.left.cte_name.clone(), graph_context.left.id_column.clone(), graph_rel.is_rel_anchor);
            rel_cte_name = r_cte_name;
            rel_ctxs_to_update = r_ctxs_to_update;
            rel_plan = r_plan;

            right_insubquery = self.build_insubquery(graph_context.right.id_column,
                rel_cte_name.clone(),
                "to_id".to_string());

            left_insubquery = self.build_insubquery(graph_context.left.id_column,
                rel_cte_name.clone(),
                "from_id".to_string());
        }

        if graph_rel.is_rel_anchor {
            let right_ctx_to_update = CtxToUpdate {
                alias: graph_context.right.alias.to_string(),
                label: graph_context.right.label,
                projections: right_projections,
                insubquery: Some(right_insubquery),
                override_projections: false,
                is_rel: true
            };
            ctxs_to_update.push(right_ctx_to_update);

            rel_ctxs_to_update.first_mut().unwrap().insubquery = None;

            // let rel_ctx_to_update = CtxToUpdate {
            //     alias: graph_context.rel.alias.to_string(),
            //     label: graph_context.rel.label,
            //     projections: rel_projections,
            //     insubquery: None,
            //     override_projections: false
            // };
            ctxs_to_update.append(&mut rel_ctxs_to_update);

            let left_ctx_to_update = CtxToUpdate {
                alias: graph_context.left.alias.to_string(),
                label: graph_context.left.label,
                projections: left_projections,
                insubquery: Some(left_insubquery),
                override_projections: false,
                is_rel: false
            };
            ctxs_to_update.push(left_ctx_to_update);

            let new_graph_rel = GraphRel { 
                left: Arc::new(LogicalPlan::Cte(Cte { input: graph_rel.left.clone(), name: graph_context.left.cte_name })),
                // center: Arc::new(LogicalPlan::Cte(Cte { input: graph_rel.center.clone(), name: graph_context.right.cte_name })),
                // right: Arc::new(LogicalPlan::Cte(Cte { input: graph_rel.right.clone(), name: rel_cte_name  })), 
                center: Arc::new(LogicalPlan::Cte(Cte { input: graph_rel.right.clone(), name: graph_context.right.cte_name })),
                right: Arc::new(LogicalPlan::Cte(Cte { input: rel_plan.clone(), name: rel_cte_name  })), 
                ..graph_rel.clone()
            };

            return Ok((new_graph_rel, ctxs_to_update))

        }else{

            // let rel_ctx_to_update = CtxToUpdate {
            //     alias: graph_context.rel.alias.to_string(),
            //     label: graph_context.rel.label,
            //     projections: rel_projections,
            //     insubquery: Some(rel_insubquery),
            //     override_projections: false
            // };
            ctxs_to_update.append(&mut rel_ctxs_to_update);

            let left_ctx_to_update = CtxToUpdate {
                alias: graph_context.left.alias.to_string(),
                label: graph_context.left.label,
                projections: left_projections,
                insubquery: Some(left_insubquery),
                override_projections: false,
                is_rel: false
            };
            ctxs_to_update.push(left_ctx_to_update);

            if is_anchor_traversal {
                let right_ctx_to_update = CtxToUpdate {
                    alias: graph_context.right.alias.to_string(),
                    label: graph_context.right.label,
                    projections: right_projections,
                    insubquery: None,
                    override_projections: false,
                    is_rel: false
                };
                ctxs_to_update.push(right_ctx_to_update);

                let new_graph_rel = GraphRel { 
                    left: Arc::new(LogicalPlan::Cte(Cte { input: graph_rel.left.clone(), name: graph_context.left.cte_name })),
                    center: Arc::new(LogicalPlan::Cte(Cte { input: rel_plan.clone(), name: rel_cte_name })),
                    right: Arc::new(LogicalPlan::Cte(Cte { input: graph_rel.right.clone(), name: graph_context.right.cte_name })), 
                    ..graph_rel.clone()
                };
                return Ok((new_graph_rel, ctxs_to_update))
            } else {
                let new_graph_rel = GraphRel { 
                    left: Arc::new(LogicalPlan::Cte(Cte { input: graph_rel.left.clone(), name: graph_context.left.cte_name })),
                    center: Arc::new(LogicalPlan::Cte(Cte { input: rel_plan.clone(), name: rel_cte_name })),
                    right: graph_rel.right.clone(), 
                    ..graph_rel.clone()
                };
    
                return Ok((new_graph_rel, ctxs_to_update))
            }
        }
    }

    fn handle_bitmap_traversal(&self, graph_rel: &GraphRel, graph_context: GraphContext, left_projections: Vec<ProjectionItem>, right_projections: Vec<ProjectionItem>, is_anchor_traversal:bool) -> AnalyzerResult<(GraphRel, Vec<CtxToUpdate>)> {

        let mut ctxs_to_update: Vec<CtxToUpdate> = vec![];

        // let new_rel_label = self.get_relationship_table_name(graph_context.right.label.clone(), graph_context.left.label.clone(), graph_context.rel.label, graph_rel.direction.clone(), graph_context.rel.schema)?;
        // let rel_cte_name = format!("{}_{}", new_rel_label, graph_context.rel.alias);

        // let rel_proj_input: Vec<(String, Option<ColumnAlias>)> = vec![
        //     ("from_id".to_string(), None),
        //     ("arrayJoin(bitmapToArray(to_id))".to_string(), Some(ColumnAlias("to_id".to_string())))
        // ];
        // let rel_projections = self.build_projections(rel_proj_input);
        // let rel_insubquery = self.build_insubquery("from_id".to_string(),
        //     graph_context.right.cte_name.clone(),
        //     graph_context.right.id_column.clone());

        
        // let rel_ctx_to_update = CtxToUpdate {
        //     alias: graph_context.rel.alias.to_string(),
        //     label: new_rel_label,
        //     projections: rel_projections,
        //     insubquery: Some(rel_insubquery),
        //     override_projections: false,
        //     is_rel: true
        // };
        // ctxs_to_update.push(rel_ctx_to_update);

        let (rel_cte_name, rel_plan, mut rel_ctxs_to_update) = self.get_rel_ctx_for_bitmaps(&graph_rel, &graph_context, graph_context.right.cte_name.clone(), graph_context.right.id_column.clone());
        
        ctxs_to_update.append(&mut rel_ctxs_to_update);

        let left_insubquery = self.build_insubquery(graph_context.left.id_column,
            rel_cte_name.clone(),
            "to_id".to_string());
        let left_ctx_to_update = CtxToUpdate {
            alias: graph_context.left.alias.to_string(),
            label: graph_context.left.label,
            projections: left_projections,
            insubquery: Some(left_insubquery),
            override_projections: false,
            is_rel: false
        };
        ctxs_to_update.push(left_ctx_to_update);

        if is_anchor_traversal {
            let right_ctx_to_update = CtxToUpdate {
                alias: graph_context.right.alias.to_string(),
                label: graph_context.right.label,
                projections: right_projections,
                insubquery: None,
                override_projections: false,
                is_rel: false
            };
            ctxs_to_update.push(right_ctx_to_update);

            let new_graph_rel = GraphRel { 
                left: Arc::new(LogicalPlan::Cte(Cte { input: graph_rel.left.clone(), name: graph_context.left.cte_name })),
                center: Arc::new(LogicalPlan::Cte(Cte { input: rel_plan, name: rel_cte_name })),
                right: Arc::new(LogicalPlan::Cte(Cte { input: graph_rel.right.clone(), name: graph_context.right.cte_name })), 
                ..graph_rel.clone()
            };
    
            Ok((new_graph_rel, ctxs_to_update))
        } else {
            let new_graph_rel = GraphRel { 
                left: Arc::new(LogicalPlan::Cte(Cte { input: graph_rel.left.clone(), name: graph_context.left.cte_name })),
                center: Arc::new(LogicalPlan::Cte(Cte { input: rel_plan, name: rel_cte_name })),
                right: graph_rel.right.clone(), 
                ..graph_rel.clone()
            };
            Ok((new_graph_rel, ctxs_to_update))
        }

        

    }

    
    fn get_rel_ctx_for_edge_list(&self, graph_rel: &GraphRel, graph_context: &GraphContext, connected_node_cte_name: String, connected_node_id_column: String, is_rel_anchor: bool) -> (String, Arc<LogicalPlan>, Vec<CtxToUpdate>) {

        
        // let rel_cte_name = format!("{}_{}", graph_context.rel.label, graph_context.rel.alias);

        let star_found = graph_context.rel.table_ctx.get_projections().iter().any(|item| item.expression == LogicalExpr::Star);
        
        // let rel_insubquery:LogicalExpr;

        // let rel_projections:Vec<ProjectionItem>;

        // let mut override_projections = false;

        

        // if direction == Direction::Either and both nodes are of same types then use UNION of both.
        // TODO - currently Either direction on anchor relation is not supported. FIX this
        if graph_rel.direction == Direction::Either && graph_context.left.label == graph_context.right.label && !is_rel_anchor { 
            // let new_rel_label = format!("{}_{}", graph_context.rel.label, Direction::Either); //"Direction::Either);

            let rel_cte_name = format!("{}_{}", graph_context.rel.label, graph_context.rel.alias);

            let outgoing_alias = logical_plan::generate_id();
            let incoming_alias = logical_plan::generate_id();

            // let outgoing_label = format!("{}_{}", graph_context.rel.label, Direction::Outgoing);
            // let incoming_label = format!("{}_{}", graph_context.rel.label, Direction::Incoming);

            let rel_plan: Arc<LogicalPlan> = Arc::new(LogicalPlan::Union(Union{
                inputs: vec![
                    Arc::new(LogicalPlan::Scan(Scan { table_alias: Some(outgoing_alias.clone()), table_name: Some(graph_context.rel.label.clone()) })),
                    Arc::new(LogicalPlan::Scan(Scan { table_alias: Some(incoming_alias.clone()), table_name: Some(graph_context.rel.label.clone()) }))
                ]
            }));

            let rel_insubquery: LogicalExpr = self.build_insubquery("from_id".to_string(),
                connected_node_cte_name.clone(),
                connected_node_id_column.clone());

            let from_edge_proj_input: Vec<(String, Option<ColumnAlias>)> = if !star_found {
                vec![
                    (format!("from_{}", graph_context.rel.schema.from_node), Some(ColumnAlias("from_id".to_string()))),
                    (format!("to_{}", graph_context.rel.schema.to_node), Some(ColumnAlias("to_id".to_string())))
                ]
            } else { vec![] };
    
            let from_edge_projections = self.build_projections(from_edge_proj_input);

            let from_edge_ctx_to_update = CtxToUpdate {
                alias: outgoing_alias,
                label: graph_context.rel.label.clone(),
                projections: from_edge_projections,
                insubquery: Some(rel_insubquery.clone()),
                override_projections: false,
                is_rel: true,
            };

            let to_edge_proj_input: Vec<(String, Option<ColumnAlias>)> = if !star_found {
                vec![
                    (format!("to_{}", graph_context.rel.schema.from_node), Some(ColumnAlias("from_id".to_string()))),
                    (format!("from_{}", graph_context.rel.schema.to_node), Some(ColumnAlias("to_id".to_string())))
                ]
            } else { vec![] };
    
            let to_edge_projections = self.build_projections(to_edge_proj_input);


            let to_edge_ctx_to_update = CtxToUpdate {
                alias: incoming_alias,
                label: graph_context.rel.label.clone(),
                projections: to_edge_projections,
                insubquery: Some(rel_insubquery),
                override_projections: false,
                is_rel: true,
            };

            return (rel_cte_name, rel_plan, vec![from_edge_ctx_to_update, to_edge_ctx_to_update])
        } else {

            let rel_cte_name = format!("{}_{}", graph_context.rel.label.clone(), graph_context.rel.alias);

            let rel_proj_input: Vec<(String, Option<ColumnAlias>)> = if !star_found {
                vec![
                    (format!("from_{}", graph_context.rel.schema.from_node), Some(ColumnAlias("from_id".to_string()))),
                    (format!("to_{}", graph_context.rel.schema.to_node), Some(ColumnAlias("to_id".to_string())))
                ]
            } else { vec![] };
    
            let rel_projections = self.build_projections(rel_proj_input);


            let sub_in_expr_str = if graph_rel.direction == Direction::Outgoing {
                "from_id".to_string()
            } else {
                "to_id".to_string()
            };

            let rel_insubquery = self.build_insubquery(sub_in_expr_str,
            connected_node_cte_name,
            connected_node_id_column);

            let rel_plan = graph_rel.center.clone();

            let rel_ctx_to_update = CtxToUpdate {
                alias: graph_context.rel.alias.to_string(),
                label: graph_context.rel.label.clone(),
                projections: rel_projections,
                insubquery: Some(rel_insubquery),
                override_projections: false,
                is_rel: true
            };

            return (rel_cte_name, rel_plan, vec![rel_ctx_to_update])

        }
    
        // // if direction == Direction::Either and both nodes are of same types then use UNION of both.
        // if graph_rel.direction == Direction::Either && graph_context.left.label == graph_context.right.label { 

        //     if star_found {
        //         rel_projections = vec![ProjectionItem {
        //                 expression: LogicalExpr::OperatorApplicationExp(OperatorApplication { operator: Operator::Distinct, operands:  vec![LogicalExpr::Star]}),
        //                 col_alias: None
        //             }
        //         ];
        //     } else {
        //         rel_projections = vec![
        //             // ProjectionItem {
        //             //     expression: LogicalExpr::OperatorApplicationExp(OperatorApplication { operator: Operator::Distinct, operands:  vec![LogicalExpr::Column(Column(format!("from_{}", graph_context.rel.schema.from_node)))]}),
        //             //     col_alias: Some(ColumnAlias("from_id".to_string()))
        //             // }, 
        //             ProjectionItem {
        //                 expression: LogicalExpr::Column(Column(format!("from_{}", graph_context.rel.schema.from_node))),
        //                 col_alias: Some(ColumnAlias("from_id".to_string()))
        //             },
        //             ProjectionItem {
        //                 expression: LogicalExpr::Column(Column(format!("to_{}", graph_context.rel.schema.from_node))),
        //                 col_alias: Some(ColumnAlias("to_id".to_string()))
        //             }
        //         ];
        //     }

        //     override_projections = true;

        //     let rel_from_insubquery = self.build_insubquery("from_id".to_string(),
        //     connected_node_cte_name.clone(),
        //     connected_node_id_column.clone());
        //     // graph_context.right.cte_name.clone(),
        //     // graph_context.right.id_column.clone());

        //     let rel_to_insubquery = self.build_insubquery("to_id".to_string(),
        //     connected_node_cte_name,
        //     connected_node_id_column);

        //     rel_insubquery = LogicalExpr::OperatorApplicationExp(OperatorApplication{
        //         operator: Operator::Or,
        //         operands: vec![rel_from_insubquery, rel_to_insubquery]
        //     });

        // } else if graph_context.rel.schema.from_node == graph_context.right.schema.table_name {
        //     let rel_proj_input: Vec<(String, Option<ColumnAlias>)> = if !star_found {
        //         vec![
        //             (format!("from_{}", graph_context.rel.schema.from_node), Some(ColumnAlias("from_id".to_string()))),
        //             (format!("to_{}", graph_context.rel.schema.to_node), Some(ColumnAlias("to_id".to_string())))
        //         ]
        //     } else { vec![] };
    
        //     rel_projections = self.build_projections(rel_proj_input);

        //     rel_insubquery = self.build_insubquery("from_id".to_string(),
        //     connected_node_cte_name,
        //     connected_node_id_column);

        // } else {
        //     let rel_proj_input: Vec<(String, Option<ColumnAlias>)> = if !star_found {
        //         vec![
        //             (format!("from_{}", graph_context.rel.schema.from_node), Some(ColumnAlias("from_id".to_string()))),
        //             (format!("to_{}", graph_context.rel.schema.to_node), Some(ColumnAlias("to_id".to_string())))
        //         ]
        //     } else { vec![] };
    
        //     rel_projections = self.build_projections(rel_proj_input);

        //     rel_insubquery = self.build_insubquery("to_id".to_string(),
        //     connected_node_cte_name,
        //     connected_node_id_column);

        // }

        // let rel_ctx_to_update = CtxToUpdate {
        //     alias: graph_context.rel.alias.to_string(),
        //     label: graph_context.rel.label.clone(),
        //     projections: rel_projections,
        //     insubquery: Some(rel_insubquery),
        //     override_projections: override_projections,
        //     is_rel: true
        // };
        
        // (rel_cte_name, rel_ctx_to_update)
        
    }
    
    fn get_rel_ctx_for_bitmaps(&self, graph_rel: &GraphRel, graph_context: &GraphContext, connected_node_cte_name: String, connected_node_id_column: String) -> (String, Arc<LogicalPlan>, Vec<CtxToUpdate>){
        

        let rel_proj_input: Vec<(String, Option<ColumnAlias>)> = vec![
                ("from_id".to_string(), None),
                ("arrayJoin(bitmapToArray(to_id))".to_string(), Some(ColumnAlias("to_id".to_string())))
            ];
        let rel_projections = self.build_projections(rel_proj_input);

        // if direction == Direction::Either and both nodes are of same types then use UNION of both.
        if graph_rel.direction == Direction::Either && graph_context.left.label == graph_context.right.label {
            let new_rel_label = format!("{}_{}", graph_context.rel.label, Direction::Either); //"Direction::Either);

            let rel_cte_name = format!("{}_{}", new_rel_label, graph_context.rel.alias);

            let outgoing_alias = logical_plan::generate_id();
            let incoming_alias = logical_plan::generate_id();

            let outgoing_label = format!("{}_{}", graph_context.rel.label, Direction::Outgoing);
            let incoming_label = format!("{}_{}", graph_context.rel.label, Direction::Incoming);

            let rel_plan: Arc<LogicalPlan> = Arc::new(LogicalPlan::Union(Union{
                inputs: vec![
                    Arc::new(LogicalPlan::Scan(Scan { table_alias: Some(outgoing_alias.clone()), table_name: Some(outgoing_label.clone()) })),
                    Arc::new(LogicalPlan::Scan(Scan { table_alias: Some(incoming_alias.clone()), table_name: Some(incoming_label.clone()) }))
                ]
            }));
            

            let rel_insubquery = self.build_insubquery("from_id".to_string(),
                connected_node_cte_name,
                connected_node_id_column);

            let outgoing_ctx_to_update = CtxToUpdate {
                alias: outgoing_alias.clone(),
                label: outgoing_label,
                projections: rel_projections.clone(),
                insubquery: Some(rel_insubquery.clone()),
                override_projections: false,
                is_rel: true,
            };


            let incoming_ctx_to_update = CtxToUpdate {
                alias: incoming_alias.clone(),
                label: incoming_label,
                projections: rel_projections.clone(),
                insubquery: Some(rel_insubquery),
                override_projections: false,
                is_rel: true,
            };

            let existing_rel_ctx_to_update = CtxToUpdate {
                alias: graph_context.rel.alias.to_string(),
                label: new_rel_label, // just update the label so that in graph join inference we can derive the cte name
                projections: vec![],
                insubquery: None,
                override_projections: false,
                is_rel: true
            };

            return (rel_cte_name, rel_plan, vec![existing_rel_ctx_to_update, outgoing_ctx_to_update, incoming_ctx_to_update])


        } else{
            let index_direction  = if graph_context.left.label == graph_context.right.label {
                graph_rel.direction.clone()
            }  else if graph_context.rel.schema.from_node == graph_context.right.schema.table_name {
                Direction::Outgoing
            } else { 
                Direction::Incoming
            };
            let new_rel_label = format!("{}_{}", graph_context.rel.label, index_direction);

            let rel_cte_name = format!("{}_{}", new_rel_label, graph_context.rel.alias);

            let rel_insubquery = self.build_insubquery("from_id".to_string(),
                connected_node_cte_name,
                connected_node_id_column);

            let rel_plan = graph_rel.center.clone();

            let ctx_to_update = CtxToUpdate {
                alias: graph_context.rel.alias.to_string(),
                label: new_rel_label,
                projections: rel_projections,
                insubquery: Some(rel_insubquery.clone()),
                override_projections: false,
                is_rel: true,
            };

            return (rel_cte_name, rel_plan, vec![ctx_to_update])
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
                table_alias: None,
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

        println!("direction {:?}", direction);

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

        Err(AnalyzerError::NoRelationSchemaFound{ pass: Pass::GraphTraversalPlanning})
    }


    // fn infer_anchor_traversal(&self, graph_rel: &GraphRel, plan_ctx: &mut PlanCtx, graph_schema: &GraphSchema) -> AnalyzerResult<(GraphRel, Vec<CtxToUpdate>)> {
    //     // get required information 
    //     let left_alias = &graph_rel.left_connection;
    //     let rel_alias = &graph_rel.alias;
    //     let right_alias = &graph_rel.right_connection;


    //     let left_ctx = plan_ctx.get_node_table_ctx(left_alias).map_err(|e| AnalyzerError::PlanCtx { pass: Pass::GraphTraversalPlanning, source: e})?;
    //     let rel_ctx = plan_ctx.get_rel_table_ctx(rel_alias).map_err(|e| AnalyzerError::PlanCtx { pass: Pass::GraphTraversalPlanning, source: e})?;
    //     let right_ctx = plan_ctx.get_node_table_ctx(right_alias).map_err(|e| AnalyzerError::PlanCtx { pass: Pass::GraphTraversalPlanning, source: e})?;

    //     let left_label = left_ctx.get_label_str().map_err(|e| AnalyzerError::PlanCtx { pass: Pass::GraphTraversalPlanning, source: e})?;
    //     let rel_label = rel_ctx.get_label_str().map_err(|e| AnalyzerError::PlanCtx { pass: Pass::GraphTraversalPlanning, source: e})?;
    //     let right_label = right_ctx.get_label_str().map_err(|e| AnalyzerError::PlanCtx { pass: Pass::GraphTraversalPlanning, source: e})?;


    //     let left_schema = graph_schema.get_node_schema(&left_label).map_err(|e| AnalyzerError::GraphSchema { pass: Pass::GraphTraversalPlanning, source: e})?;
    //     let rel_schema = graph_schema.get_rel_schema(&rel_label).map_err(|e| AnalyzerError::GraphSchema { pass: Pass::GraphTraversalPlanning, source: e})?;
    //     let right_schema = graph_schema.get_node_schema(&right_label).map_err(|e| AnalyzerError::GraphSchema { pass: Pass::GraphTraversalPlanning, source: e})?;

    //     let left_node_id_column = left_schema.node_id.column.clone();
    //     let right_node_id_column = right_schema.node_id.column.clone();

    //     let mut ctxs_to_update: Vec<CtxToUpdate> = vec![];


    //     let left_cte_name = format!("{}_{}",left_label, left_alias);
    //     let right_cte_name = format!("{}_{}", right_label, right_alias);

    //     let star_found = right_ctx.get_projections().iter().any(|item| item.expression == LogicalExpr::Star);
    //     let node_id_found = right_ctx.get_projections().iter().any(|item| {
    //         match &item.expression {
    //             LogicalExpr::Column(Column(col)) => col == &right_node_id_column,
    //             LogicalExpr::PropertyAccessExp(PropertyAccess { column, .. }) => column.0 == right_node_id_column,
    //             _ => false,
    //         }
    //     });
    //     let right_projections: Vec<ProjectionItem> = if !star_found && !node_id_found {
    //         let proj_input: Vec<(String, Option<ColumnAlias>)> = vec![(right_node_id_column.clone(), None)];
    //         self.build_projections(proj_input)
    //     } else {
    //         vec![]
    //     };
        

    //     let star_found = left_ctx.get_projections().iter().any(|item| item.expression == LogicalExpr::Star);
    //     let node_id_found = left_ctx.get_projections().iter().any(|item| {
    //         match &item.expression {
    //             LogicalExpr::Column(Column(col)) => col == &left_node_id_column,
    //             LogicalExpr::PropertyAccessExp(PropertyAccess { column, .. }) => column.0 == left_node_id_column,
    //             _ => false,
    //         }
    //     });
    //     let left_projections: Vec<ProjectionItem> = if !star_found && !node_id_found {
    //         let proj_input: Vec<(String, Option<ColumnAlias>)> = vec![(left_node_id_column.clone(), None)];
    //         self.build_projections(proj_input)
    //     } else {
    //         vec![]
    //     };

    //     if rel_ctx.should_use_edge_list() {
    //         let rel_cte_name = format!("{}_{}", rel_label, rel_alias);
            
    //         let star_found = rel_ctx.get_projections().iter().any(|item| item.expression == LogicalExpr::Star);
    //         let rel_proj_input: Vec<(String, Option<ColumnAlias>)> = if !star_found {
    //             vec![
    //                 (format!("from_{}", rel_schema.from_node), Some(ColumnAlias("from_id".to_string()))),
    //                 (format!("to_{}", rel_schema.to_node.clone()), Some(ColumnAlias("to_id".to_string())))
    //             ]
    //         } else { vec![] };

    //         let rel_projections:Vec<ProjectionItem> = self.build_projections(rel_proj_input);
    //         let rel_insubquery: LogicalExpr;
    //         let right_insubquery: LogicalExpr;
    //         let left_insubquery: LogicalExpr;
    //         // when using edge list, we need to check which node joins to "from_id" and which node joins to "to_id"
    //         if rel_schema.from_node == right_schema.table_name {
    //             rel_insubquery = self.build_insubquery("from_id".to_string(),
    //             right_cte_name.clone(),
    //             right_node_id_column.clone());

    //             right_insubquery = self.build_insubquery(right_node_id_column,
    //                 rel_cte_name.clone(),
    //                 "from_id".to_string());

    //             left_insubquery = self.build_insubquery(left_node_id_column,
    //                 rel_cte_name.clone(),
    //                 "to_id".to_string());
                
    //         }else{
    //             rel_insubquery = self.build_insubquery("to_id".to_string(),
    //             right_cte_name.clone(),
    //             right_node_id_column.clone());

    //             right_insubquery = self.build_insubquery(right_node_id_column,
    //                 rel_cte_name.clone(),
    //                 "to_id".to_string());

    //             left_insubquery = self.build_insubquery(left_node_id_column,
    //                 rel_cte_name.clone(),
    //                 "from_id".to_string());
    //         }

    //         if graph_rel.is_rel_anchor {
    //             let right_ctx_to_update = CtxToUpdate {
    //                 alias: right_alias.to_string(),
    //                 label: right_label,
    //                 projections: right_projections,
    //                 insubquery: Some(right_insubquery),
    //             };
    //             ctxs_to_update.push(right_ctx_to_update);

    //             let rel_ctx_to_update = CtxToUpdate {
    //                 alias: rel_alias.to_string(),
    //                 label: rel_label,
    //                 projections: rel_projections,
    //                 insubquery: None,
    //             };
    //             ctxs_to_update.push(rel_ctx_to_update);

    //             let left_ctx_to_update = CtxToUpdate {
    //                 alias: left_alias.to_string(),
    //                 label: left_label,
    //                 projections: left_projections,
    //                 insubquery: Some(left_insubquery),
    //             };
    //             ctxs_to_update.push(left_ctx_to_update);

    //             let new_graph_rel = GraphRel { 
    //                 left: Arc::new(LogicalPlan::Cte(Cte { input: graph_rel.left.clone(), name: left_cte_name })),
    //                 center: Arc::new(LogicalPlan::Cte(Cte { input: graph_rel.center.clone(), name: right_cte_name })),
    //                 right: Arc::new(LogicalPlan::Cte(Cte { input: graph_rel.right.clone(), name: rel_cte_name  })), 
    //                 ..graph_rel.clone()
    //             };
    //             return Ok((new_graph_rel, ctxs_to_update))

    //         }else{
    //             let right_ctx_to_update = CtxToUpdate {
    //                 alias: right_alias.to_string(),
    //                 label: right_label,
    //                 projections: right_projections,
    //                 insubquery: None,
    //             };
    //             ctxs_to_update.push(right_ctx_to_update);

    //             let rel_ctx_to_update = CtxToUpdate {
    //                 alias: rel_alias.to_string(),
    //                 label: rel_label,
    //                 projections: rel_projections,
    //                 insubquery: Some(rel_insubquery),
    //             };
    //             ctxs_to_update.push(rel_ctx_to_update);

    //             let left_ctx_to_update = CtxToUpdate {
    //                 alias: left_alias.to_string(),
    //                 label: left_label,
    //                 projections: left_projections,
    //                 insubquery: Some(left_insubquery),
    //             };
    //             ctxs_to_update.push(left_ctx_to_update);


    //             let new_graph_rel = GraphRel { 
    //                 left: Arc::new(LogicalPlan::Cte(Cte { input: graph_rel.left.clone(), name: left_cte_name })),
    //                 center: Arc::new(LogicalPlan::Cte(Cte { input: graph_rel.center.clone(), name: rel_cte_name })),
    //                 right: Arc::new(LogicalPlan::Cte(Cte { input: graph_rel.right.clone(), name: right_cte_name })), 
    //                 ..graph_rel.clone()
    //             };
    
    //             return Ok((new_graph_rel, ctxs_to_update))
    //         }


            
    //     } else {
           

    //         let new_rel_label = self.get_relationship_table_name(right_label.clone(), left_label.clone(), rel_label, graph_rel.direction.clone(), rel_schema)?;
    //         let rel_cte_name = format!("{}_{}", new_rel_label, rel_alias);

    //         let right_ctx_to_update = CtxToUpdate {
    //             alias: right_alias.to_string(),
    //             label: right_label,
    //             projections: right_projections,
    //             insubquery: None,
    //         };
    //         ctxs_to_update.push(right_ctx_to_update);

    //         let rel_proj_input: Vec<(String, Option<ColumnAlias>)> = vec![
    //             ("from_id".to_string(), None),
    //             ("arrayJoin(bitmapToArray(to_id))".to_string(), Some(ColumnAlias("to_id".to_string())))
    //         ];
    //         let rel_projections = self.build_projections(rel_proj_input);
    //         let rel_insubquery = self.build_insubquery("from_id".to_string(),
    //             right_cte_name.clone(),
    //             right_node_id_column);
    //         let rel_ctx_to_update = CtxToUpdate {
    //             alias: rel_alias.to_string(),
    //             label: new_rel_label,
    //             projections: rel_projections,
    //             insubquery: Some(rel_insubquery),
    //         };
    //         ctxs_to_update.push(rel_ctx_to_update);
            

    //         let left_insubquery = self.build_insubquery(left_node_id_column,
    //             rel_cte_name.clone(),
    //             "to_id".to_string());
    //         let left_ctx_to_update = CtxToUpdate {
    //             alias: left_alias.to_string(),
    //             label: left_label,
    //             projections: left_projections,
    //             insubquery: Some(left_insubquery),
    //         };
    //         ctxs_to_update.push(left_ctx_to_update);

    //         let new_graph_rel = GraphRel { 
    //             left: Arc::new(LogicalPlan::Cte(Cte { input: graph_rel.left.clone(), name: left_cte_name })),
    //             center: Arc::new(LogicalPlan::Cte(Cte { input: graph_rel.center.clone(), name: rel_cte_name })),
    //             right: Arc::new(LogicalPlan::Cte(Cte { input: graph_rel.right.clone(), name: right_cte_name })), 
    //             ..graph_rel.clone()
    //         };

    //         Ok((new_graph_rel, ctxs_to_update))
    //     }

    // }

    // fn infer_intermediate_traversal(&self, graph_rel: &GraphRel, plan_ctx: &mut PlanCtx, graph_schema: &GraphSchema) -> AnalyzerResult<(GraphRel, Vec<CtxToUpdate>)> {
    //     // get required information 
    //     let left_alias = &graph_rel.left_connection;
    //     let rel_alias = &graph_rel.alias;
    //     let right_alias = &graph_rel.right_connection;


    //     let left_ctx = plan_ctx.get_node_table_ctx(left_alias).map_err(|e| AnalyzerError::PlanCtx { pass: Pass::GraphTraversalPlanning, source: e})?;
    //     let rel_ctx = plan_ctx.get_rel_table_ctx(rel_alias).map_err(|e| AnalyzerError::PlanCtx { pass: Pass::GraphTraversalPlanning, source: e})?;
    //     let right_ctx = plan_ctx.get_node_table_ctx(right_alias).map_err(|e| AnalyzerError::PlanCtx { pass: Pass::GraphTraversalPlanning, source: e})?;

    //     let left_label = left_ctx.get_label_str().map_err(|e| AnalyzerError::PlanCtx { pass: Pass::GraphTraversalPlanning, source: e})?;
    //     let rel_label = rel_ctx.get_label_str().map_err(|e| AnalyzerError::PlanCtx { pass: Pass::GraphTraversalPlanning, source: e})?;
    //     let right_label = right_ctx.get_label_str().map_err(|e| AnalyzerError::PlanCtx { pass: Pass::GraphTraversalPlanning, source: e})?;


    //     let left_schema = graph_schema.get_node_schema(&left_label).map_err(|e| AnalyzerError::GraphSchema { pass: Pass::GraphTraversalPlanning, source: e})?;
    //     let rel_schema = graph_schema.get_rel_schema(&rel_label).map_err(|e| AnalyzerError::GraphSchema { pass: Pass::GraphTraversalPlanning, source: e})?;
    //     let right_schema = graph_schema.get_node_schema(&right_label).map_err(|e| AnalyzerError::GraphSchema { pass: Pass::GraphTraversalPlanning, source: e})?;

    //     let left_node_id_column = left_schema.node_id.column.clone();
    //     let right_node_id_column = right_schema.node_id.column.clone();

    //     let mut ctxs_to_update: Vec<CtxToUpdate> = vec![];


    //     let left_cte_name = format!("{}_{}",left_label, left_alias);
    //     let right_cte_name = format!("{}_{}", right_label, right_alias);


    //     let star_found = left_ctx.get_projections().iter().any(|item| item.expression == LogicalExpr::Star);
    //     let node_id_found = left_ctx.get_projections().iter().any(|item| {
    //         match &item.expression {
    //             LogicalExpr::Column(Column(col)) => col == &left_node_id_column,
    //             LogicalExpr::PropertyAccessExp(PropertyAccess { column, .. }) => column.0 == left_node_id_column,
    //             _ => false,
    //         }
    //     });
    //     let left_projections: Vec<ProjectionItem> = if !star_found && !node_id_found {
    //         let proj_input: Vec<(String, Option<ColumnAlias>)> = vec![(left_node_id_column.clone(), None)];
    //         self.build_projections(proj_input)
    //     } else {
    //         vec![]
    //     };


    //     if rel_ctx.should_use_edge_list() {
    //         let rel_cte_name = format!("{}_{}", rel_label, rel_alias);
            
    //         let star_found = rel_ctx.get_projections().iter().any(|item| item.expression == LogicalExpr::Star);
    //         let rel_proj_input: Vec<(String, Option<ColumnAlias>)> = if !star_found {
    //             vec![
    //                 (format!("from_{}", rel_schema.from_node), Some(ColumnAlias("from_id".to_string()))),
    //                 (format!("to_{}", rel_schema.to_node.clone()), Some(ColumnAlias("to_id".to_string())))
    //             ]
    //         } else { vec![] };

    //         let rel_projections:Vec<ProjectionItem> = self.build_projections(rel_proj_input);
    //         let rel_insubquery: LogicalExpr;
    //         // let right_insubquery: LogicalExpr;
    //         let left_insubquery: LogicalExpr;
    //         // when using edge list, we need to check which node joins to "from_id" and which node joins to "to_id"
    //         if rel_schema.from_node == right_schema.table_name {
    //             rel_insubquery = self.build_insubquery("from_id".to_string(),
    //             right_cte_name.clone(),
    //             right_node_id_column.clone());

    //             left_insubquery = self.build_insubquery(left_node_id_column,
    //                 rel_cte_name.clone(),
    //                 "to_id".to_string());
                
    //         }else{
    //             rel_insubquery = self.build_insubquery("to_id".to_string(),
    //             right_cte_name.clone(),
    //             right_node_id_column.clone());

    //             left_insubquery = self.build_insubquery(left_node_id_column,
    //                 rel_cte_name.clone(),
    //                 "from_id".to_string());
    //         }

    //         let rel_ctx_to_update = CtxToUpdate {
    //             alias: rel_alias.to_string(),
    //             label: rel_cte_name.to_string(),
    //             projections: rel_projections,
    //             insubquery: Some(rel_insubquery),
    //         };
    //         ctxs_to_update.push(rel_ctx_to_update);

    //         let left_ctx_to_update = CtxToUpdate {
    //             alias: left_alias.to_string(),
    //             label: left_label,
    //             projections: left_projections,
    //             insubquery: Some(left_insubquery),
    //         };
    //         ctxs_to_update.push(left_ctx_to_update);


    //         let new_graph_rel = GraphRel { 
    //             left: Arc::new(LogicalPlan::Cte(Cte { input: graph_rel.left.clone(), name: left_cte_name })),
    //             center: Arc::new(LogicalPlan::Cte(Cte { input: graph_rel.center.clone(), name: rel_cte_name })),
    //             right: graph_rel.right.clone(), 
    //             ..graph_rel.clone()
    //         };

    //         return Ok((new_graph_rel, ctxs_to_update))
            
    //     } else{

    //         let new_rel_label = self.get_relationship_table_name(right_label.clone(), left_label.clone(), rel_label, graph_rel.direction.clone(), rel_schema)?;
    //         let rel_cte_name = format!("{}_{}", new_rel_label, rel_alias);


    //         let rel_proj_input: Vec<(String, Option<ColumnAlias>)> = vec![
    //             ("from_id".to_string(), None),
    //             ("arrayJoin(bitmapToArray(to_id))".to_string(), Some(ColumnAlias("to_id".to_string())))
    //         ];
    //         let rel_projections = self.build_projections(rel_proj_input);
    //         let rel_insubquery = self.build_insubquery("from_id".to_string(),
    //             right_cte_name.clone(),
    //             right_node_id_column);
    //         let rel_ctx_to_update = CtxToUpdate {
    //             alias: rel_alias.to_string(),
    //             label: new_rel_label,
    //             projections: rel_projections,
    //             insubquery: Some(rel_insubquery),
    //         };
    //         ctxs_to_update.push(rel_ctx_to_update);
            

    //         let left_insubquery = self.build_insubquery(left_node_id_column,
    //             rel_cte_name.clone(),
    //             "to_id".to_string());
    //         let left_ctx_to_update = CtxToUpdate {
    //             alias: left_alias.to_string(),
    //             label: left_label,
    //             projections: left_projections,
    //             insubquery: Some(left_insubquery),
    //         };
    //         ctxs_to_update.push(left_ctx_to_update);

    //         let new_graph_rel = GraphRel { 
    //             left: Arc::new(LogicalPlan::Cte(Cte { input: graph_rel.left.clone(), name: left_cte_name })),
    //             center: Arc::new(LogicalPlan::Cte(Cte { input: graph_rel.center.clone(), name: rel_cte_name })),
    //             right: graph_rel.right.clone(), 
    //             ..graph_rel.clone()
    //         };
            

    //         Ok((new_graph_rel, ctxs_to_update))
    //     }


    // }
}