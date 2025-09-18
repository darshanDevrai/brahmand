use std::sync::Arc;

use crate::{
    graph_catalog::graph_schema::GraphSchema,
    query_planner::{
        analyzer::{
            analyzer_pass::{AnalyzerPass, AnalyzerResult},
            errors::Pass,
            graph_context::{self, GraphContext},
        },
        logical_expr::{Column, ColumnAlias, Direction, InSubquery, LogicalExpr, PropertyAccess},
        logical_plan::{
            self, {Cte, GraphRel, LogicalPlan, Projection, ProjectionItem, Scan, Union, UnionType},
        },
        plan_ctx::{PlanCtx, TableCtx},
        transformed::Transformed,
    },
};

pub struct GraphTRaversalPlanning;

impl AnalyzerPass for GraphTRaversalPlanning {
    fn analyze_with_graph_schema(
        &self,
        logical_plan: Arc<LogicalPlan>,
        plan_ctx: &mut PlanCtx,
        graph_schema: &GraphSchema,
    ) -> AnalyzerResult<Transformed<Arc<LogicalPlan>>> {
        let transformed_plan = match logical_plan.as_ref() {
            LogicalPlan::Projection(projection) => {
                let child_tf = self.analyze_with_graph_schema(
                    projection.input.clone(),
                    plan_ctx,
                    graph_schema,
                )?;
                projection.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::GraphNode(graph_node) => {
                let child_tf = self.analyze_with_graph_schema(
                    graph_node.input.clone(),
                    plan_ctx,
                    graph_schema,
                )?;
                graph_node.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::GraphRel(graph_rel) => {
                // If no graphRel at the right means we have reached at the bottom of the tree i.e. right is anchor.
                if !matches!(graph_rel.right.as_ref(), LogicalPlan::GraphRel(_)) {
                    let (new_graph_rel, ctxs_to_update) =
                        self.infer_traversal(graph_rel, plan_ctx, graph_schema, true)?;

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
                            let mut new_table_ctx = TableCtx::build(
                                ctx.alias.clone(),
                                Some(ctx.label),
                                vec![],
                                ctx.is_rel,
                                false,
                            );
                            if let Some(plan_expr) = ctx.insubquery {
                                new_table_ctx.insert_filter(plan_expr);
                            }
                            new_table_ctx.set_projections(ctx.projections);

                            plan_ctx.insert_table_ctx(ctx.alias.clone(), new_table_ctx);
                        }
                    }

                    Transformed::Yes(Arc::new(LogicalPlan::GraphRel(new_graph_rel)))
                } else {
                    let right_tf = self.analyze_with_graph_schema(
                        graph_rel.right.clone(),
                        plan_ctx,
                        graph_schema,
                    )?;

                    let updated_graph_rel = GraphRel {
                        right: right_tf.get_plan(),
                        ..graph_rel.clone()
                    };
                    let (new_graph_rel, ctxs_to_update) =
                        self.infer_traversal(&updated_graph_rel, plan_ctx, graph_schema, false)?;

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
                            let mut new_table_ctx = TableCtx::build(
                                ctx.alias.clone(),
                                Some(ctx.label),
                                vec![],
                                ctx.is_rel,
                                false,
                            );
                            if let Some(plan_expr) = ctx.insubquery {
                                new_table_ctx.insert_filter(plan_expr);
                            }
                            new_table_ctx.set_projections(ctx.projections);

                            plan_ctx.insert_table_ctx(ctx.alias.clone(), new_table_ctx);
                        }
                    }

                    Transformed::Yes(Arc::new(LogicalPlan::GraphRel(new_graph_rel)))
                }
            }
            LogicalPlan::Cte(cte) => {
                let child_tf =
                    self.analyze_with_graph_schema(cte.input.clone(), plan_ctx, graph_schema)?;
                cte.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Scan(_) => Transformed::No(logical_plan.clone()),
            LogicalPlan::Empty => Transformed::No(logical_plan.clone()),
            LogicalPlan::GraphJoins(graph_joins) => {
                let child_tf = self.analyze_with_graph_schema(
                    graph_joins.input.clone(),
                    plan_ctx,
                    graph_schema,
                )?;
                graph_joins.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Filter(filter) => {
                let child_tf =
                    self.analyze_with_graph_schema(filter.input.clone(), plan_ctx, graph_schema)?;
                filter.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::GroupBy(group_by) => {
                let child_tf =
                    self.analyze_with_graph_schema(group_by.input.clone(), plan_ctx, graph_schema)?;
                group_by.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::OrderBy(order_by) => {
                let child_tf =
                    self.analyze_with_graph_schema(order_by.input.clone(), plan_ctx, graph_schema)?;
                order_by.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Skip(skip) => {
                let child_tf =
                    self.analyze_with_graph_schema(skip.input.clone(), plan_ctx, graph_schema)?;
                skip.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Limit(limit) => {
                let child_tf =
                    self.analyze_with_graph_schema(limit.input.clone(), plan_ctx, graph_schema)?;
                limit.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Union(union) => {
                let mut inputs_tf: Vec<Transformed<Arc<LogicalPlan>>> = vec![];
                for input_plan in union.inputs.iter() {
                    let child_tf =
                        self.analyze_with_graph_schema(input_plan.clone(), plan_ctx, graph_schema)?;
                    inputs_tf.push(child_tf);
                }
                union.rebuild_or_clone(inputs_tf, logical_plan.clone())
            }
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
    is_rel: bool,
}

impl GraphTRaversalPlanning {
    pub fn new() -> Self {
        GraphTRaversalPlanning
    }

    fn infer_traversal(
        &self,
        graph_rel: &GraphRel,
        plan_ctx: &mut PlanCtx,
        graph_schema: &GraphSchema,
        is_anchor_traversal: bool,
    ) -> AnalyzerResult<(GraphRel, Vec<CtxToUpdate>)> {
        let graph_context = graph_context::get_graph_context(
            graph_rel,
            plan_ctx,
            graph_schema,
            Pass::GraphTraversalPlanning,
        )?;

        // left is traversed irrespective of anchor node or intermediate node
        let star_found = graph_context
            .left
            .table_ctx
            .get_projections()
            .iter()
            .any(|item| item.expression == LogicalExpr::Star);
        let node_id_found = graph_context
            .left
            .table_ctx
            .get_projections()
            .iter()
            .any(|item| match &item.expression {
                LogicalExpr::Column(Column(col)) => col == &graph_context.left.id_column,
                LogicalExpr::PropertyAccessExp(PropertyAccess { column, .. }) => {
                    column.0 == graph_context.left.id_column
                }
                _ => false,
            });
        let left_projections: Vec<ProjectionItem> = if !star_found && !node_id_found {
            let proj_input: Vec<(String, Option<ColumnAlias>)> =
                vec![(graph_context.left.id_column.clone(), None)];
            self.build_projections(proj_input)
        } else {
            vec![]
        };

        let star_found = graph_context
            .right
            .table_ctx
            .get_projections()
            .iter()
            .any(|item| item.expression == LogicalExpr::Star);
        let node_id_found = graph_context
            .right
            .table_ctx
            .get_projections()
            .iter()
            .any(|item| match &item.expression {
                LogicalExpr::Column(Column(col)) => col == &graph_context.right.id_column,
                LogicalExpr::PropertyAccessExp(PropertyAccess { column, .. }) => {
                    column.0 == graph_context.right.id_column
                }
                _ => false,
            });
        let right_projections: Vec<ProjectionItem> = if !star_found && !node_id_found {
            let proj_input: Vec<(String, Option<ColumnAlias>)> =
                vec![(graph_context.right.id_column.clone(), None)];
            self.build_projections(proj_input)
        } else {
            vec![]
        };

        if graph_context.rel.table_ctx.should_use_edge_list() {
            self.handle_edge_list_traversal(
                graph_rel,
                graph_context,
                left_projections,
                right_projections,
                is_anchor_traversal,
            )
        } else {
            self.handle_bitmap_traversal(
                graph_rel,
                graph_context,
                left_projections,
                right_projections,
                is_anchor_traversal,
            )
        }
    }

    fn handle_edge_list_traversal(
        &self,
        graph_rel: &GraphRel,
        graph_context: GraphContext,
        left_projections: Vec<ProjectionItem>,
        right_projections: Vec<ProjectionItem>,
        is_anchor_traversal: bool,
    ) -> AnalyzerResult<(GraphRel, Vec<CtxToUpdate>)> {
        let mut ctxs_to_update: Vec<CtxToUpdate> = vec![];

        let mut rel_ctxs_to_update: Vec<CtxToUpdate>;

        let (r_cte_name, r_plan, r_ctxs_to_update) = self.get_rel_ctx_for_edge_list(
            graph_rel,
            &graph_context,
            graph_context.right.cte_name.clone(),
            graph_context.right.id_column.clone(),
            graph_rel.is_rel_anchor,
        );
        let rel_cte_name: String = r_cte_name;
        rel_ctxs_to_update = r_ctxs_to_update;
        let rel_plan: Arc<LogicalPlan> = r_plan;

        // when using edge list, we need to check which node joins to "from_id" and which node joins to "to_id" of the relationship.
        // Based on that we decide, how the left and right nodes are connected with relationship in subqueries.
        let (right_sub_plan_column, left_sub_plan_column) =
            if graph_context.rel.schema.from_node == graph_context.right.schema.table_name {
                ("from_id".to_string(), "to_id".to_string())
            } else {
                ("to_id".to_string(), "from_id".to_string())
            };

        let right_insubquery: LogicalExpr = self.build_insubquery(
            graph_context.right.id_column.clone(),
            rel_cte_name.clone(),
            right_sub_plan_column,
        );

        let left_insubquery: LogicalExpr = self.build_insubquery(
            graph_context.left.id_column,
            rel_cte_name.clone(),
            left_sub_plan_column,
        );

        if graph_rel.is_rel_anchor {
            let right_ctx_to_update = CtxToUpdate {
                alias: graph_context.right.alias.to_string(),
                label: graph_context.right.label,
                projections: right_projections,
                insubquery: Some(right_insubquery),
                override_projections: false,
                is_rel: true,
            };
            ctxs_to_update.push(right_ctx_to_update);

            rel_ctxs_to_update.first_mut().unwrap().insubquery = None;

            ctxs_to_update.append(&mut rel_ctxs_to_update);

            let left_ctx_to_update = CtxToUpdate {
                alias: graph_context.left.alias.to_string(),
                label: graph_context.left.label,
                projections: left_projections,
                insubquery: Some(left_insubquery),
                override_projections: false,
                is_rel: false,
            };
            ctxs_to_update.push(left_ctx_to_update);

            let new_graph_rel = GraphRel {
                left: Arc::new(LogicalPlan::Cte(Cte {
                    input: graph_rel.left.clone(),
                    name: graph_context.left.cte_name,
                })),
                center: Arc::new(LogicalPlan::Cte(Cte {
                    input: graph_rel.right.clone(),
                    name: graph_context.right.cte_name,
                })),
                right: Arc::new(LogicalPlan::Cte(Cte {
                    input: rel_plan.clone(),
                    name: rel_cte_name,
                })),
                ..graph_rel.clone()
            };

            Ok((new_graph_rel, ctxs_to_update))
        } else {
            ctxs_to_update.append(&mut rel_ctxs_to_update);

            let left_ctx_to_update = CtxToUpdate {
                alias: graph_context.left.alias.to_string(),
                label: graph_context.left.label,
                projections: left_projections,
                insubquery: Some(left_insubquery),
                override_projections: false,
                is_rel: false,
            };
            ctxs_to_update.push(left_ctx_to_update);

            if is_anchor_traversal {
                let right_ctx_to_update = CtxToUpdate {
                    alias: graph_context.right.alias.to_string(),
                    label: graph_context.right.label,
                    projections: right_projections,
                    insubquery: None,
                    override_projections: false,
                    is_rel: false,
                };
                ctxs_to_update.push(right_ctx_to_update);

                let new_graph_rel = GraphRel {
                    left: Arc::new(LogicalPlan::Cte(Cte {
                        input: graph_rel.left.clone(),
                        name: graph_context.left.cte_name,
                    })),
                    center: Arc::new(LogicalPlan::Cte(Cte {
                        input: rel_plan.clone(),
                        name: rel_cte_name,
                    })),
                    right: Arc::new(LogicalPlan::Cte(Cte {
                        input: graph_rel.right.clone(),
                        name: graph_context.right.cte_name,
                    })),
                    ..graph_rel.clone()
                };
                Ok((new_graph_rel, ctxs_to_update))
            } else {
                let new_graph_rel = GraphRel {
                    left: Arc::new(LogicalPlan::Cte(Cte {
                        input: graph_rel.left.clone(),
                        name: graph_context.left.cte_name,
                    })),
                    center: Arc::new(LogicalPlan::Cte(Cte {
                        input: rel_plan.clone(),
                        name: rel_cte_name,
                    })),
                    right: graph_rel.right.clone(),
                    ..graph_rel.clone()
                };

                Ok((new_graph_rel, ctxs_to_update))
            }
        }
    }

    fn handle_bitmap_traversal(
        &self,
        graph_rel: &GraphRel,
        graph_context: GraphContext,
        left_projections: Vec<ProjectionItem>,
        right_projections: Vec<ProjectionItem>,
        is_anchor_traversal: bool,
    ) -> AnalyzerResult<(GraphRel, Vec<CtxToUpdate>)> {
        let mut ctxs_to_update: Vec<CtxToUpdate> = vec![];

        let (rel_cte_name, rel_plan, mut rel_ctxs_to_update) = self.get_rel_ctx_for_bitmaps(
            graph_rel,
            &graph_context,
            graph_context.right.cte_name.clone(),
            graph_context.right.id_column.clone(),
        );

        ctxs_to_update.append(&mut rel_ctxs_to_update);

        let left_insubquery = self.build_insubquery(
            graph_context.left.id_column,
            rel_cte_name.clone(),
            "to_id".to_string(),
        );
        let left_ctx_to_update = CtxToUpdate {
            alias: graph_context.left.alias.to_string(),
            label: graph_context.left.label,
            projections: left_projections,
            insubquery: Some(left_insubquery),
            override_projections: false,
            is_rel: false,
        };
        ctxs_to_update.push(left_ctx_to_update);

        if is_anchor_traversal {
            let right_ctx_to_update = CtxToUpdate {
                alias: graph_context.right.alias.to_string(),
                label: graph_context.right.label,
                projections: right_projections,
                insubquery: None,
                override_projections: false,
                is_rel: false,
            };
            ctxs_to_update.push(right_ctx_to_update);

            let new_graph_rel = GraphRel {
                left: Arc::new(LogicalPlan::Cte(Cte {
                    input: graph_rel.left.clone(),
                    name: graph_context.left.cte_name,
                })),
                center: Arc::new(LogicalPlan::Cte(Cte {
                    input: rel_plan,
                    name: rel_cte_name,
                })),
                right: Arc::new(LogicalPlan::Cte(Cte {
                    input: graph_rel.right.clone(),
                    name: graph_context.right.cte_name,
                })),
                ..graph_rel.clone()
            };

            Ok((new_graph_rel, ctxs_to_update))
        } else {
            let new_graph_rel = GraphRel {
                left: Arc::new(LogicalPlan::Cte(Cte {
                    input: graph_rel.left.clone(),
                    name: graph_context.left.cte_name,
                })),
                center: Arc::new(LogicalPlan::Cte(Cte {
                    input: rel_plan,
                    name: rel_cte_name,
                })),
                right: graph_rel.right.clone(),
                ..graph_rel.clone()
            };
            Ok((new_graph_rel, ctxs_to_update))
        }
    }

    fn get_rel_ctx_for_edge_list(
        &self,
        graph_rel: &GraphRel,
        graph_context: &GraphContext,
        connected_node_cte_name: String,
        connected_node_id_column: String,
        is_rel_anchor: bool,
    ) -> (String, Arc<LogicalPlan>, Vec<CtxToUpdate>) {
        let star_found = graph_context
            .rel
            .table_ctx
            .get_projections()
            .iter()
            .any(|item| item.expression == LogicalExpr::Star);

        // if direction == Direction::Either and both nodes are of same types then use UNION of both.
        // TODO - currently Either direction on anchor relation is not supported. FIX this
        if graph_rel.direction == Direction::Either
            && graph_context.left.label == graph_context.right.label
            && !is_rel_anchor
        {
            // let new_rel_label = format!("{}_{}", graph_context.rel.label, Direction::Either); //"Direction::Either);

            let rel_cte_name = format!("{}_{}", graph_context.rel.label, graph_context.rel.alias);

            let outgoing_alias = logical_plan::generate_id();
            let incoming_alias = logical_plan::generate_id();

            // let outgoing_label = format!("{}_{}", graph_context.rel.label, Direction::Outgoing);
            // let incoming_label = format!("{}_{}", graph_context.rel.label, Direction::Incoming);

            let rel_plan: Arc<LogicalPlan> = Arc::new(LogicalPlan::Union(Union {
                inputs: vec![
                    Arc::new(LogicalPlan::Scan(Scan {
                        table_alias: Some(outgoing_alias.clone()),
                        table_name: Some(graph_context.rel.label.clone()),
                    })),
                    Arc::new(LogicalPlan::Scan(Scan {
                        table_alias: Some(incoming_alias.clone()),
                        table_name: Some(graph_context.rel.label.clone()),
                    })),
                ],
                union_type: UnionType::Distinct,
            }));

            let rel_insubquery: LogicalExpr = self.build_insubquery(
                "from_id".to_string(),
                connected_node_cte_name.clone(),
                connected_node_id_column.clone(),
            );

            let from_edge_proj_input: Vec<(String, Option<ColumnAlias>)> = if !star_found {
                vec![
                    (
                        format!("from_{}", graph_context.rel.schema.from_node),
                        Some(ColumnAlias("from_id".to_string())),
                    ),
                    (
                        format!("to_{}", graph_context.rel.schema.to_node),
                        Some(ColumnAlias("to_id".to_string())),
                    ),
                ]
            } else {
                vec![]
            };

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
                    (
                        format!("to_{}", graph_context.rel.schema.from_node),
                        Some(ColumnAlias("from_id".to_string())),
                    ),
                    (
                        format!("from_{}", graph_context.rel.schema.to_node),
                        Some(ColumnAlias("to_id".to_string())),
                    ),
                ]
            } else {
                vec![]
            };

            let to_edge_projections = self.build_projections(to_edge_proj_input);

            let to_edge_ctx_to_update = CtxToUpdate {
                alias: incoming_alias,
                label: graph_context.rel.label.clone(),
                projections: to_edge_projections,
                insubquery: Some(rel_insubquery),
                override_projections: false,
                is_rel: true,
            };

            (
                rel_cte_name,
                rel_plan,
                vec![from_edge_ctx_to_update, to_edge_ctx_to_update],
            )
        } else {
            let rel_cte_name = format!(
                "{}_{}",
                graph_context.rel.label.clone(),
                graph_context.rel.alias
            );

            let rel_proj_input: Vec<(String, Option<ColumnAlias>)> = if !star_found {
                vec![
                    (
                        format!("from_{}", graph_context.rel.schema.from_node),
                        Some(ColumnAlias("from_id".to_string())),
                    ),
                    (
                        format!("to_{}", graph_context.rel.schema.to_node),
                        Some(ColumnAlias("to_id".to_string())),
                    ),
                ]
            } else {
                vec![]
            };

            let rel_projections = self.build_projections(rel_proj_input);

            // when using edge list, we need to check which node joins to "from_id" and which node joins to "to_id" of the relationship.
            // Based on that we decide, how the relationship is connected with right node as we traverse in graph traversal planning from right to left i.e. bottom to top.
            // Relationship direction integrity is already checked during query validation. If there is wrong direction then plan won't come to this stage. So we don't have to check direction here.
            let sub_in_expr_str =
                if graph_context.rel.schema.from_node == graph_context.right.schema.table_name {
                    "from_id".to_string()
                } else {
                    "to_id".to_string()
                };

            // let sub_in_expr_str = if graph_rel.direction == Direction::Outgoing {
            //     "from_id".to_string()
            // } else {
            //     "to_id".to_string()
            // };

            let rel_insubquery = self.build_insubquery(
                sub_in_expr_str,
                connected_node_cte_name,
                connected_node_id_column,
            );

            let rel_plan = graph_rel.center.clone();

            let rel_ctx_to_update = CtxToUpdate {
                alias: graph_context.rel.alias.to_string(),
                label: graph_context.rel.label.clone(),
                projections: rel_projections,
                insubquery: Some(rel_insubquery),
                override_projections: false,
                is_rel: true,
            };

            (rel_cte_name, rel_plan, vec![rel_ctx_to_update])
        }
    }

    fn get_rel_ctx_for_bitmaps(
        &self,
        graph_rel: &GraphRel,
        graph_context: &GraphContext,
        connected_node_cte_name: String,
        connected_node_id_column: String,
    ) -> (String, Arc<LogicalPlan>, Vec<CtxToUpdate>) {
        let rel_proj_input: Vec<(String, Option<ColumnAlias>)> = vec![
            ("from_id".to_string(), None),
            (
                "arrayJoin(bitmapToArray(to_id))".to_string(),
                Some(ColumnAlias("to_id".to_string())),
            ),
        ];
        let rel_projections = self.build_projections(rel_proj_input);

        // if direction == Direction::Either and both nodes are of same types then use UNION of both.
        if graph_rel.direction == Direction::Either
            && graph_context.left.label == graph_context.right.label
        {
            let new_rel_label = format!("{}_{}", graph_context.rel.label, Direction::Either); //"Direction::Either);

            let rel_cte_name = format!("{}_{}", new_rel_label, graph_context.rel.alias);

            let outgoing_alias = logical_plan::generate_id();
            let incoming_alias = logical_plan::generate_id();

            let outgoing_label = format!("{}_{}", graph_context.rel.label, Direction::Outgoing);
            let incoming_label = format!("{}_{}", graph_context.rel.label, Direction::Incoming);

            let rel_plan: Arc<LogicalPlan> = Arc::new(LogicalPlan::Union(Union {
                inputs: vec![
                    Arc::new(LogicalPlan::Scan(Scan {
                        table_alias: Some(outgoing_alias.clone()),
                        table_name: Some(outgoing_label.clone()),
                    })),
                    Arc::new(LogicalPlan::Scan(Scan {
                        table_alias: Some(incoming_alias.clone()),
                        table_name: Some(incoming_label.clone()),
                    })),
                ],
                union_type: UnionType::Distinct,
            }));

            let rel_insubquery = self.build_insubquery(
                "from_id".to_string(),
                connected_node_cte_name,
                connected_node_id_column,
            );

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
                is_rel: true,
            };

            (
                rel_cte_name,
                rel_plan,
                vec![
                    existing_rel_ctx_to_update,
                    outgoing_ctx_to_update,
                    incoming_ctx_to_update,
                ],
            )
        } else {
            let index_direction = if graph_rel.direction == Direction::Either
                && graph_context.rel.schema.from_node == graph_context.right.schema.table_name
            {
                Direction::Outgoing
            } else if graph_rel.direction == Direction::Either
                && graph_context.rel.schema.to_node == graph_context.right.schema.table_name
            {
                Direction::Incoming
            } else {
                graph_rel.direction.clone()
            };

            // let index_direction  = if graph_context.left.label == graph_context.right.label {
            //     graph_rel.direction.clone()
            // }  else if graph_context.rel.schema.from_node == graph_context.right.schema.table_name {
            //     Direction::Outgoing
            // } else {
            //     Direction::Incoming
            // };
            let new_rel_label = format!("{}_{}", graph_context.rel.label, index_direction);

            let rel_cte_name = format!("{}_{}", new_rel_label, graph_context.rel.alias);

            let rel_insubquery = self.build_insubquery(
                "from_id".to_string(),
                connected_node_cte_name,
                connected_node_id_column,
            );

            let rel_plan = graph_rel.center.clone();

            let ctx_to_update = CtxToUpdate {
                alias: graph_context.rel.alias.to_string(),
                label: new_rel_label,
                projections: rel_projections,
                insubquery: Some(rel_insubquery.clone()),
                override_projections: false,
                is_rel: true,
            };

            (rel_cte_name, rel_plan, vec![ctx_to_update])
        }
    }

    fn build_projections(&self, items: Vec<(String, Option<ColumnAlias>)>) -> Vec<ProjectionItem> {
        items
            .into_iter()
            .map(|(expr_str, alias)| ProjectionItem {
                expression: LogicalExpr::Column(Column(expr_str)),
                col_alias: alias,
            })
            .collect()
    }

    fn build_insubquery(
        &self,
        sub_in_exp: String,
        sub_plan_table: String,
        sub_plan_column: String,
    ) -> LogicalExpr {
        LogicalExpr::InSubquery(InSubquery {
            expr: Box::new(LogicalExpr::Column(Column(sub_in_exp))),
            subplan: self.get_subplan(sub_plan_table, sub_plan_column),
        })
    }

    fn get_subplan(&self, table_name: String, table_column: String) -> Arc<LogicalPlan> {
        Arc::new(LogicalPlan::Projection(Projection {
            input: Arc::new(LogicalPlan::Scan(Scan {
                table_alias: None,
                table_name: Some(table_name),
            })),
            items: vec![ProjectionItem {
                expression: LogicalExpr::Column(Column(table_column)),
                col_alias: None,
            }],
        }))
    }
}
