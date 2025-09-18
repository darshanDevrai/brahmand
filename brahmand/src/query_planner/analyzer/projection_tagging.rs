use std::sync::Arc;

use crate::{
    graph_catalog::graph_schema::GraphSchema,
    query_planner::{
        analyzer::{
            analyzer_pass::{AnalyzerPass, AnalyzerResult},
            errors::{AnalyzerError, Pass},
        },
        logical_expr::{
            AggregateFnCall, Column, LogicalExpr, PropertyAccess, TableAlias,
        },
        logical_plan::{LogicalPlan, Projection, ProjectionItem},
        plan_ctx::PlanCtx,
        transformed::Transformed,
    },
};

pub struct ProjectionTagging;

impl AnalyzerPass for ProjectionTagging {
    // Check if the projection item is only * then check for explicitly mentioned aliases and add * as their projection.
    // in the final projection, put all explicit alias.*

    // If there is any projection on relationship then use edgelist of that relation.
    fn analyze_with_graph_schema(
        &self,
        logical_plan: Arc<LogicalPlan>,
        plan_ctx: &mut PlanCtx,
        graph_schema: &GraphSchema,
    ) -> AnalyzerResult<Transformed<Arc<LogicalPlan>>> {
        let transformed_plan = match logical_plan.as_ref() {
            LogicalPlan::Projection(projection) => {
                // handler select all. e.g. -
                //
                // MATCH (u:User)-[c:Created]->(p:Post)
                //      RETURN *;
                //
                // We will treat it as -
                //
                // MATCH (u:User)-[c:Created]->(p:Post)
                // RETURN u, c, p;
                //
                // To achieve this we will convert `RETURN *` into `RETURN u, c, p`
                let mut proj_items_to_mutate: Vec<ProjectionItem> =
                    if self.select_all_present(&projection.items) {
                        // we will create projection items with only table alias as return item. tag_projection will handle the proper tagging and overall projection manupulation.
                        let explicit_aliases = self.get_explicit_aliases(plan_ctx);
                        explicit_aliases
                            .iter()
                            .map(|exp_alias| {
                                let table_alias = TableAlias(exp_alias.clone());
                                ProjectionItem {
                                    expression: LogicalExpr::TableAlias(table_alias.clone()),
                                    col_alias: None,
                                }
                            })
                            .collect()
                    } else {
                        projection.items.clone()
                    };

                for item in &mut proj_items_to_mutate {
                    Self::tag_projection(item, plan_ctx, graph_schema)?;
                }

                Transformed::Yes(Arc::new(LogicalPlan::Projection(Projection {
                    input: projection.input.clone(),
                    items: proj_items_to_mutate,
                })))
            }
            LogicalPlan::GraphNode(graph_node) => {
                let child_tf = self.analyze_with_graph_schema(
                    graph_node.input.clone(),
                    plan_ctx,
                    graph_schema,
                )?;
                // let self_tf = self.analyze_with_graph_schema(graph_node.self_plan.clone(), plan_ctx);
                graph_node.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::GraphRel(graph_rel) => {
                let left_tf =
                    self.analyze_with_graph_schema(graph_rel.left.clone(), plan_ctx, graph_schema)?;
                let center_tf = self.analyze_with_graph_schema(
                    graph_rel.center.clone(),
                    plan_ctx,
                    graph_schema,
                )?;
                let right_tf = self.analyze_with_graph_schema(
                    graph_rel.right.clone(),
                    plan_ctx,
                    graph_schema,
                )?;
                graph_rel.rebuild_or_clone(left_tf, center_tf, right_tf, logical_plan.clone())
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

impl ProjectionTagging {
    pub fn new() -> Self {
        ProjectionTagging
    }

    fn select_all_present(&self, projection_items: &[ProjectionItem]) -> bool {
        projection_items
            .iter()
            .any(|item| item.expression == LogicalExpr::Star)
    }

    fn get_explicit_aliases(&self, plan_ctx: &mut PlanCtx) -> Vec<String> {
        plan_ctx
            .get_alias_table_ctx_map()
            .iter()
            .filter_map(|(alias, table_ctx)| {
                if table_ctx.is_explicit_alias() {
                    Some(alias.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    fn tag_projection(
        item: &mut ProjectionItem,
        plan_ctx: &mut PlanCtx,
        graph_schema: &GraphSchema,
    ) -> AnalyzerResult<()> {
        match item.expression.clone() {
            LogicalExpr::TableAlias(table_alias) => {
                // if just table alias i.e MATCH (p:Post) Return p; then For final overall projection keep p.* and for p's projection keep *.

                let table_ctx = plan_ctx.get_mut_table_ctx(&table_alias.0).map_err(|e| {
                    AnalyzerError::PlanCtx {
                        pass: Pass::ProjectionTagging,
                        source: e,
                    }
                })?;
                let tagged_proj = ProjectionItem {
                    expression: LogicalExpr::Star,
                    col_alias: None,
                    // belongs_to_table: Some(table_alias.clone()),
                };
                // table_ctx.projection_items = vec![tagged_proj];
                table_ctx.set_projections(vec![tagged_proj]);

                // if table_ctx is of relation then mark use_edge_list = true
                if table_ctx.is_relation() {
                    table_ctx.set_use_edge_list(true);
                }

                // update the overall projection
                item.expression = LogicalExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: table_alias.clone(),
                    column: Column("*".to_string()),
                });
                Ok(())
            }
            LogicalExpr::PropertyAccessExp(property_access) => {
                let table_ctx = plan_ctx
                    .get_mut_table_ctx(&property_access.table_alias.0)
                    .map_err(|e| AnalyzerError::PlanCtx {
                        pass: Pass::ProjectionTagging,
                        source: e,
                    })?;
                table_ctx.insert_projection(item.clone());
                Ok(())
            }
            LogicalExpr::OperatorApplicationExp(operator_application) => {
                for operand in &operator_application.operands {
                    let mut operand_return_item = ProjectionItem {
                        expression: operand.clone(),
                        col_alias: None,
                    };
                    Self::tag_projection(&mut operand_return_item, plan_ctx, graph_schema)?;
                }
                Ok(())
            }
            LogicalExpr::ScalarFnCall(scalar_fn_call) => {
                for arg in &scalar_fn_call.args {
                    let mut arg_return_item = ProjectionItem {
                        expression: arg.clone(),
                        col_alias: None,
                    };
                    Self::tag_projection(&mut arg_return_item, plan_ctx, graph_schema)?;
                }
                Ok(())
            }
            // For now I am not tagging Aggregate fns, but I will tag later for aggregate pushdown when I implement the aggregate push down optimization
            // For now if there is a tableAlias in agg fn args and fn name is Count then convert the table alias to node Id
            LogicalExpr::AggregateFnCall(aggregate_fn_call) => {
                for arg in &aggregate_fn_call.args {
                    if let LogicalExpr::TableAlias(TableAlias(t_alias)) = arg {
                        if aggregate_fn_call.name.to_lowercase() == "count" {
                            let table_ctx = plan_ctx.get_mut_table_ctx(t_alias).map_err(|e| {
                                AnalyzerError::PlanCtx {
                                    pass: Pass::ProjectionTagging,
                                    source: e,
                                }
                            })?;
                            let table_label =
                                table_ctx
                                    .get_label_str()
                                    .map_err(|e| AnalyzerError::PlanCtx {
                                        pass: Pass::ProjectionTagging,
                                        source: e,
                                    })?;
                            let table_schema =
                                graph_schema.get_node_schema(&table_label).map_err(|e| {
                                    AnalyzerError::GraphSchema {
                                        pass: Pass::ProjectionTagging,
                                        source: e,
                                    }
                                })?;
                            let table_node_id = table_schema.node_id.column.clone();
                            item.expression = LogicalExpr::AggregateFnCall(AggregateFnCall {
                                name: aggregate_fn_call.name.clone(),
                                args: vec![LogicalExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: TableAlias(t_alias.to_string()),
                                    column: Column(table_node_id),
                                })],
                            });
                        }
                    }
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }
}
