use std::sync::Arc;

use crate::{
    graph_catalog::graph_schema::{GraphSchema, RelationshipSchema},
    query_planner::{
        analyzer::{
            analyzer_pass::{AnalyzerPass, AnalyzerResult},
            errors::{AnalyzerError, Pass},
        },
        logical_expr::LogicalExpr,
        logical_plan::{LogicalPlan, ProjectionItem, Scan},
        plan_ctx::{PlanCtx, TableCtx},
        transformed::Transformed,
    },
};

pub struct SchemaInference;

impl AnalyzerPass for SchemaInference {
    fn analyze_with_graph_schema(
        &self,
        logical_plan: Arc<LogicalPlan>,
        plan_ctx: &mut PlanCtx,
        graph_schema: &GraphSchema,
    ) -> AnalyzerResult<Transformed<Arc<LogicalPlan>>> {
        self.infer_schema(logical_plan.clone(), plan_ctx, graph_schema)?;

        Self::push_inferred_table_names_to_scan(logical_plan, plan_ctx)
    }
}

impl SchemaInference {
    pub fn new() -> Self {
        SchemaInference
    }

    pub fn push_inferred_table_names_to_scan(
        logical_plan: Arc<LogicalPlan>,
        plan_ctx: &mut PlanCtx,
    ) -> AnalyzerResult<Transformed<Arc<LogicalPlan>>> {
        let transformed_plan = match logical_plan.as_ref() {
            LogicalPlan::Projection(projection) => {
                let child_tf =
                    Self::push_inferred_table_names_to_scan(projection.input.clone(), plan_ctx)?;
                projection.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::GraphNode(graph_node) => {
                let child_tf =
                    Self::push_inferred_table_names_to_scan(graph_node.input.clone(), plan_ctx)?;
                graph_node.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::GraphRel(graph_rel) => {
                let left_tf =
                    Self::push_inferred_table_names_to_scan(graph_rel.left.clone(), plan_ctx)?;
                let center_tf =
                    Self::push_inferred_table_names_to_scan(graph_rel.center.clone(), plan_ctx)?;
                let right_tf =
                    Self::push_inferred_table_names_to_scan(graph_rel.right.clone(), plan_ctx)?;
                graph_rel.rebuild_or_clone(left_tf, center_tf, right_tf, logical_plan.clone())
            }
            LogicalPlan::Cte(cte) => {
                let child_tf =
                    Self::push_inferred_table_names_to_scan(cte.input.clone(), plan_ctx)?;
                cte.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Scan(scan) => {
                let table_ctx = plan_ctx
                    .get_table_ctx_from_alias_opt(&scan.table_alias)
                    .map_err(|e| AnalyzerError::PlanCtx {
                        pass: Pass::SchemaInference,
                        source: e,
                    })?;
                Transformed::Yes(Arc::new(LogicalPlan::Scan(Scan {
                    table_name: table_ctx.get_label_opt(),
                    table_alias: scan.table_alias.clone(),
                })))
            }
            LogicalPlan::Empty => Transformed::No(logical_plan.clone()),
            LogicalPlan::GraphJoins(graph_joins) => {
                let child_tf =
                    Self::push_inferred_table_names_to_scan(graph_joins.input.clone(), plan_ctx)?;
                graph_joins.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Filter(filter) => {
                let child_tf =
                    Self::push_inferred_table_names_to_scan(filter.input.clone(), plan_ctx)?;
                filter.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::GroupBy(group_by) => {
                let child_tf =
                    Self::push_inferred_table_names_to_scan(group_by.input.clone(), plan_ctx)?;
                group_by.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::OrderBy(order_by) => {
                let child_tf =
                    Self::push_inferred_table_names_to_scan(order_by.input.clone(), plan_ctx)?;
                order_by.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Skip(skip) => {
                let child_tf =
                    Self::push_inferred_table_names_to_scan(skip.input.clone(), plan_ctx)?;
                skip.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Limit(limit) => {
                let child_tf =
                    Self::push_inferred_table_names_to_scan(limit.input.clone(), plan_ctx)?;
                limit.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Union(union) => {
                let mut inputs_tf: Vec<Transformed<Arc<LogicalPlan>>> = vec![];
                for input_plan in union.inputs.iter() {
                    let child_tf =
                        Self::push_inferred_table_names_to_scan(input_plan.clone(), plan_ctx)?;
                    inputs_tf.push(child_tf);
                }
                union.rebuild_or_clone(inputs_tf, logical_plan.clone())
            }
        };
        Ok(transformed_plan)
    }

    fn infer_schema(
        &self,
        logical_plan: Arc<LogicalPlan>,
        plan_ctx: &mut PlanCtx,
        graph_schema: &GraphSchema,
    ) -> AnalyzerResult<()> {
        match logical_plan.as_ref() {
            LogicalPlan::Projection(projection) => {
                // let child_tf = self.infer_schema(projection.input.clone(), plan_ctx, graph_schema);
                // projection.rebuild_or_clone(child_tf, logical_plan.clone())
                self.infer_schema(projection.input.clone(), plan_ctx, graph_schema)
            }
            LogicalPlan::GraphNode(graph_node) => {
                // let child_tf = self.infer_schema(graph_node.input.clone(), plan_ctx, graph_schema);
                // graph_node.rebuild_or_clone(child_tf, logical_plan.clone())
                self.infer_schema(graph_node.input.clone(), plan_ctx, graph_schema)
            }
            LogicalPlan::GraphRel(graph_rel) => {
                let left_alias = &graph_rel.left_connection;
                let right_alias = &graph_rel.right_connection;

                let left_table_ctx = plan_ctx.get_node_table_ctx(left_alias).map_err(|e| {
                    AnalyzerError::PlanCtx {
                        pass: Pass::SchemaInference,
                        source: e,
                    }
                })?;
                let rel_table_ctx = plan_ctx.get_rel_table_ctx(&graph_rel.alias).map_err(|e| {
                    AnalyzerError::PlanCtx {
                        pass: Pass::SchemaInference,
                        source: e,
                    }
                })?;
                let right_table_ctx = plan_ctx.get_node_table_ctx(right_alias).map_err(|e| {
                    AnalyzerError::PlanCtx {
                        pass: Pass::SchemaInference,
                        source: e,
                    }
                })?;

                let (left_label, rel_label, right_label) = self.infer_missing_labels(
                    graph_schema,
                    left_table_ctx,
                    rel_table_ctx,
                    right_table_ctx,
                )?;

                for (alias, label) in [
                    (left_alias, left_label),
                    (&graph_rel.alias, rel_label),
                    (right_alias, right_label),
                ] {
                    let table_ctx =
                        plan_ctx
                            .get_mut_table_ctx(alias)
                            .map_err(|e| AnalyzerError::PlanCtx {
                                pass: Pass::SchemaInference,
                                source: e,
                            })?;
                    table_ctx.set_label(Some(label));
                }

                // let left_tf = self.infer_schema(graph_rel.left.clone(), plan_ctx, graph_schema);
                // let center_tf = self.infer_schema(graph_rel.center.clone(), plan_ctx, graph_schema);
                // let right_tf = self.infer_schema(graph_rel.right.clone(), plan_ctx, graph_schema);
                // graph_rel.rebuild_or_clone(left_tf, center_tf, right_tf, logical_plan.clone())

                self.infer_schema(graph_rel.left.clone(), plan_ctx, graph_schema)?;
                self.infer_schema(graph_rel.center.clone(), plan_ctx, graph_schema)?;
                self.infer_schema(graph_rel.right.clone(), plan_ctx, graph_schema)?;
                Ok(())
            }
            LogicalPlan::Cte(cte) => {
                // let child_tf = self.infer_schema( cte.input.clone(), plan_ctx, graph_schema);
                // cte.rebuild_or_clone(child_tf, logical_plan.clone())
                self.infer_schema(cte.input.clone(), plan_ctx, graph_schema)
            }
            LogicalPlan::Scan(_) => Ok(()),
            LogicalPlan::Empty => Ok(()),
            LogicalPlan::GraphJoins(graph_joins) => {
                // let child_tf = self.infer_schema(graph_joins.input.clone(), plan_ctx, graph_schema);
                // graph_joins.rebuild_or_clone(child_tf, logical_plan.clone())
                self.infer_schema(graph_joins.input.clone(), plan_ctx, graph_schema)
            }
            LogicalPlan::Filter(filter) => {
                // let child_tf = self.infer_schema(filter.input.clone(), plan_ctx, graph_schema);
                // filter.rebuild_or_clone(child_tf, logical_plan.clone())
                self.infer_schema(filter.input.clone(), plan_ctx, graph_schema)
            }
            LogicalPlan::GroupBy(group_by) => {
                // let child_tf = self.infer_schema(group_by.input.clone(), plan_ctx, graph_schema);
                // group_by.rebuild_or_clone(child_tf, logical_plan.clone())
                self.infer_schema(group_by.input.clone(), plan_ctx, graph_schema)
            }
            LogicalPlan::OrderBy(order_by) => {
                // let child_tf = self.infer_schema(order_by.input.clone(), plan_ctx, graph_schema);
                // order_by.rebuild_or_clone(child_tf, logical_plan.clone())
                self.infer_schema(order_by.input.clone(), plan_ctx, graph_schema)
            }
            LogicalPlan::Skip(skip) => {
                // let child_tf = self.infer_schema(skip.input.clone(), plan_ctx, graph_schema);
                // skip.rebuild_or_clone(child_tf, logical_plan.clone())
                self.infer_schema(skip.input.clone(), plan_ctx, graph_schema)
            }
            LogicalPlan::Limit(limit) => {
                // let child_tf = self.infer_schema(limit.input.clone(), plan_ctx, graph_schema);
                // limit.rebuild_or_clone(child_tf, logical_plan.clone())
                self.infer_schema(limit.input.clone(), plan_ctx, graph_schema)
            }
            LogicalPlan::Union(union) => {
                for input_plan in union.inputs.iter() {
                    Self::push_inferred_table_names_to_scan(input_plan.clone(), plan_ctx)?;
                }
                Ok(())
            }
        }
    }

    fn infer_missing_labels(
        &self,
        graph_schema: &GraphSchema,
        left_table_ctx: &TableCtx,
        rel_table_ctx: &TableCtx,
        right_table_ctx: &TableCtx,
    ) -> AnalyzerResult<(String, String, String)> {
        // if all present
        if left_table_ctx.get_label_opt().is_some()
            && rel_table_ctx.get_label_opt().is_some()
            && right_table_ctx.get_label_opt().is_some()
        {
            let left_node_table_name =
                left_table_ctx
                    .get_label_str()
                    .map_err(|e| AnalyzerError::PlanCtx {
                        pass: Pass::SchemaInference,
                        source: e,
                    })?;
            let right_node_table_name =
                right_table_ctx
                    .get_label_str()
                    .map_err(|e| AnalyzerError::PlanCtx {
                        pass: Pass::SchemaInference,
                        source: e,
                    })?;
            let rel_table_name =
                rel_table_ctx
                    .get_label_str()
                    .map_err(|e| AnalyzerError::PlanCtx {
                        pass: Pass::SchemaInference,
                        source: e,
                    })?;
            return Ok((left_node_table_name, rel_table_name, right_node_table_name));
        }

        // only left node missing
        if left_table_ctx.get_label_opt().is_none()
            && rel_table_ctx.get_label_opt().is_some()
            && right_table_ctx.get_label_opt().is_some()
        {
            // check relation table name and infer the node
            let rel_table_name =
                rel_table_ctx
                    .get_label_str()
                    .map_err(|e| AnalyzerError::PlanCtx {
                        pass: Pass::SchemaInference,
                        source: e,
                    })?;
            let rel_schema = graph_schema.get_rel_schema(&rel_table_name).map_err(|e| {
                AnalyzerError::GraphSchema {
                    pass: Pass::SchemaInference,
                    source: e,
                }
            })?;

            let right_table_name =
                right_table_ctx
                    .get_label_str()
                    .map_err(|e| AnalyzerError::PlanCtx {
                        pass: Pass::SchemaInference,
                        source: e,
                    })?;

            let left_table_name = if right_table_name == rel_schema.from_node {
                rel_schema.to_node.clone()
            } else {
                rel_schema.from_node.clone()
            };
            return Ok((
                left_table_name,
                rel_table_name.to_string(),
                right_table_name.to_string(),
            ));
        }

        // only right node missing
        if left_table_ctx.get_label_opt().is_some()
            && rel_table_ctx.get_label_opt().is_some()
            && right_table_ctx.get_label_opt().is_none()
        {
            // check relation table name and infer the node
            let rel_table_name =
                rel_table_ctx
                    .get_label_str()
                    .map_err(|e| AnalyzerError::PlanCtx {
                        pass: Pass::SchemaInference,
                        source: e,
                    })?;
            let rel_schema = graph_schema.get_rel_schema(&rel_table_name).map_err(|e| {
                AnalyzerError::GraphSchema {
                    pass: Pass::SchemaInference,
                    source: e,
                }
            })?;

            let left_table_name =
                left_table_ctx
                    .get_label_str()
                    .map_err(|e| AnalyzerError::PlanCtx {
                        pass: Pass::SchemaInference,
                        source: e,
                    })?;

            let right_table_name = if left_table_name == rel_schema.from_node {
                rel_schema.to_node.clone()
            } else {
                rel_schema.from_node.clone()
            };
            return Ok((
                left_table_name.to_string(),
                rel_table_name.to_string(),
                right_table_name,
            ));
        }

        // only relation missing
        if left_table_ctx.get_label_opt().is_some()
            && rel_table_ctx.get_label_opt().is_none()
            && right_table_ctx.get_label_opt().is_some()
        {
            let left_table_name =
                left_table_ctx
                    .get_label_str()
                    .map_err(|e| AnalyzerError::PlanCtx {
                        pass: Pass::SchemaInference,
                        source: e,
                    })?;
            let right_table_name =
                right_table_ctx
                    .get_label_str()
                    .map_err(|e| AnalyzerError::PlanCtx {
                        pass: Pass::SchemaInference,
                        source: e,
                    })?;
            for (_, relation_schema) in graph_schema.get_relationships_schemas().iter() {
                if (relation_schema.from_node == left_table_name
                    && relation_schema.to_node == right_table_name)
                    || (relation_schema.from_node == right_table_name
                        && relation_schema.to_node == left_table_name)
                {
                    return Ok((
                        left_table_name.to_string(),
                        relation_schema.table_name.clone(),
                        right_table_name.to_string(),
                    ));
                }
            }
            return Err(AnalyzerError::MissingRelationLabel {
                pass: Pass::SchemaInference,
            });
        }

        // both left and right nodes are missing but relation is present
        if left_table_ctx.get_label_opt().is_none()
            && rel_table_ctx.get_label_opt().is_some()
            && right_table_ctx.get_label_opt().is_none()
        {
            let rel_table_name =
                rel_table_ctx
                    .get_label_str()
                    .map_err(|e| AnalyzerError::PlanCtx {
                        pass: Pass::SchemaInference,
                        source: e,
                    })?;
            let relation_schema = graph_schema.get_rel_schema(&rel_table_name).map_err(|e| {
                AnalyzerError::GraphSchema {
                    pass: Pass::SchemaInference,
                    source: e,
                }
            })?;

            let extracted_left_node_table_result =
                self.get_table_name_from_filters_and_projections(graph_schema, left_table_ctx);
            let extracted_right_node_table_result =
                self.get_table_name_from_filters_and_projections(graph_schema, right_table_ctx);
            // Check the location of extracted nodes in the rel schema because the left and right of a graph changes with direction
            if extracted_left_node_table_result.is_some() {
                let left_table_name = extracted_left_node_table_result.unwrap();

                let right_table_name = if relation_schema.from_node == left_table_name {
                    &graph_schema
                        .get_node_schema(&relation_schema.to_node)
                        .map_err(|e| AnalyzerError::GraphSchema {
                            pass: Pass::SchemaInference,
                            source: e,
                        })?
                        .table_name
                } else {
                    &graph_schema
                        .get_node_schema(&relation_schema.from_node)
                        .map_err(|e| AnalyzerError::GraphSchema {
                            pass: Pass::SchemaInference,
                            source: e,
                        })?
                        .table_name
                };
                return Ok((
                    left_table_name,
                    rel_table_name.to_string(),
                    right_table_name.to_string(),
                ));
            } else if extracted_right_node_table_result.is_some() {
                let right_table_name = extracted_right_node_table_result.unwrap();

                let left_table_name = if relation_schema.from_node == right_table_name {
                    &graph_schema
                        .get_node_schema(&relation_schema.to_node)
                        .map_err(|e| AnalyzerError::GraphSchema {
                            pass: Pass::SchemaInference,
                            source: e,
                        })?
                        .table_name
                } else {
                    &graph_schema
                        .get_node_schema(&relation_schema.from_node)
                        .map_err(|e| AnalyzerError::GraphSchema {
                            pass: Pass::SchemaInference,
                            source: e,
                        })?
                        .table_name
                };
                return Ok((
                    left_table_name.to_string(),
                    rel_table_name.to_string(),
                    right_table_name,
                ));
            } else {
                // assign default left and right from rel schema.
                let left_table_name = &graph_schema
                    .get_node_schema(&relation_schema.from_node)
                    .map_err(|e| AnalyzerError::GraphSchema {
                        pass: Pass::SchemaInference,
                        source: e,
                    })?
                    .table_name;
                let right_table_name = &graph_schema
                    .get_node_schema(&relation_schema.to_node)
                    .map_err(|e| AnalyzerError::GraphSchema {
                        pass: Pass::SchemaInference,
                        source: e,
                    })?
                    .table_name;
                return Ok((
                    left_table_name.to_string(),
                    rel_table_name.to_string(),
                    right_table_name.to_string(),
                ));
            }
        }

        // right and relation missing
        if left_table_ctx.get_label_opt().is_some()
            && rel_table_ctx.get_label_opt().is_none()
            && right_table_ctx.get_label_opt().is_none()
        {
            // If the relation is absent and other node is present then check for a relation with one node = other node which is present.
            // If multiple such relations are found then use current nodes where conditions and return items like above to infer the table name of current node
            // We do this to correctly identify the correct node. We will utilize all available data to infer the current node.
            // e.g. Suppose there are nodes USER, PLANET, TOWN, SHIP. and both PLANET and TOWN has property 'name'.
            // QUERY: (b:USER)-[]->(a) Where a.name = 'Mars'.
            // If we directly go for node's where conditions and return items then we will get two nodes PLANET and TOWN and we won't be able to decide.
            // If our graph has (USER)-[DRIVES]->(CAR) and (USER)-[IS_FROM]-(TOWN). In this case how to decide DRIVES or IS_FROM relation?
            // Now we will check if CAR or TOWN has property 'name' and infer that as a current node
            let left_table_name =
                left_table_ctx
                    .get_label_str()
                    .map_err(|e| AnalyzerError::PlanCtx {
                        pass: Pass::SchemaInference,
                        source: e,
                    })?;
            let mut relations_found: Vec<&RelationshipSchema> = vec![];

            for (_, relation_schema) in graph_schema.get_relationships_schemas().iter() {
                if relation_schema.from_node == left_table_name
                    || relation_schema.to_node == left_table_name
                {
                    relations_found.push(relation_schema);
                }
            }

            let extracted_right_node_table_result =
                self.get_table_name_from_filters_and_projections(graph_schema, right_table_ctx);

            if relations_found.len() > 1 && extracted_right_node_table_result.is_some() {
                let extracted_right_node_table_name = extracted_right_node_table_result.unwrap();
                for relation_schema in relations_found {
                    let rel_table_name = &relation_schema.table_name;
                    // if the existing left node and extracted right node table is present in the current relation
                    // then use the current relation and new right node name
                    if (relation_schema.from_node == left_table_name
                        && relation_schema.to_node == extracted_right_node_table_name)
                        || relation_schema.to_node == left_table_name
                            && relation_schema.from_node == extracted_right_node_table_name
                    {
                        let right_table_name = extracted_right_node_table_name;
                        return Ok((
                            left_table_name.to_string(),
                            rel_table_name.to_string(),
                            right_table_name.to_string(),
                        ));
                    }
                }
            }
            // Getting first relationship will mis lead the output. It is good to throw the Not enough information error.
            // else {
            //     let relation_schema = relations_found
            //         .first()
            //         .ok_or(AnalyzerError::MissingRelationLabel)?;

            //     let right_table_name = if relation_schema.from_node == left_table_name {
            //         &graph_schema.get_node_schema(&relation_schema.to_node)?.table_name
            //     } else {
            //         &graph_schema.get_node_schema(&relation_schema.from_node)?.table_name
            //     };
            //     let rel_table_name = &relation_schema.table_name;
            //     return Ok((
            //         left_table_name.to_string(),
            //         rel_table_name.to_string(),
            //         right_table_name.to_string(),
            //     ));
            // }
        }

        // left and relation missing
        // Do the same as above but for right node
        if left_table_ctx.get_label_opt().is_none()
            && rel_table_ctx.get_label_opt().is_none()
            && right_table_ctx.get_label_opt().is_some()
        {
            let right_table_name =
                right_table_ctx
                    .get_label_str()
                    .map_err(|e| AnalyzerError::PlanCtx {
                        pass: Pass::SchemaInference,
                        source: e,
                    })?;
            let mut relations_found: Vec<&RelationshipSchema> = vec![];

            for (_, relation_schema) in graph_schema.get_relationships_schemas().iter() {
                if relation_schema.from_node == right_table_name
                    || relation_schema.to_node == right_table_name
                {
                    relations_found.push(relation_schema);
                }
            }

            let extracted_left_node_table_result =
                self.get_table_name_from_filters_and_projections(graph_schema, left_table_ctx);

            if relations_found.len() > 1 && extracted_left_node_table_result.is_some() {
                let extracted_left_node_table_name = extracted_left_node_table_result.unwrap();
                for relation_schema in relations_found {
                    let rel_table_name = &relation_schema.table_name;
                    // if the existing right node is present at from_node in relation
                    // and the left node's extracted column is present in curren found relation's column names
                    // then use the current relation and new left node name

                    if (relation_schema.from_node == right_table_name
                        && relation_schema.to_node == extracted_left_node_table_name)
                        || relation_schema.to_node == right_table_name
                            && relation_schema.from_node == extracted_left_node_table_name
                    {
                        let left_table_name = extracted_left_node_table_name;
                        return Ok((
                            left_table_name.to_string(),
                            rel_table_name.to_string(),
                            right_table_name.to_string(),
                        ));
                    }
                }
            }

            // else {
            //     let relation_schema = relations_found
            //         .first()
            //         .ok_or(AnalyzerError::MissingRelationLabel)?;

            //     let left_table_name = if relation_schema.from_node == right_table_name {
            //         &graph_schema.get_node_schema(&relation_schema.to_node)?.table_name
            //     } else {
            //         &graph_schema.get_node_schema(&relation_schema.from_node)?.table_name
            //     };
            //     let rel_table_name = &relation_schema.table_name;
            //     return Ok((
            //         left_table_name.to_string(),
            //         rel_table_name.to_string(),
            //         right_table_name.to_string(),
            //     ));
            // }
        }

        // if all labels are missing
        if left_table_ctx.get_label_opt().is_none()
            && rel_table_ctx.get_label_opt().is_none()
            && right_table_ctx.get_label_opt().is_none()
        {
            let extracted_left_node_table_result =
                self.get_table_name_from_filters_and_projections(graph_schema, left_table_ctx);
            let extracted_right_node_table_result =
                self.get_table_name_from_filters_and_projections(graph_schema, right_table_ctx);
            // if both extracted nodes are present
            if extracted_left_node_table_result.is_some()
                && extracted_right_node_table_result.is_some()
            {
                let left_table_name = extracted_left_node_table_result.unwrap();
                let right_table_name = extracted_right_node_table_result.unwrap();

                for (_, relation_schema) in graph_schema.get_relationships_schemas().iter() {
                    if (relation_schema.from_node == left_table_name
                        && relation_schema.to_node == right_table_name)
                        || (relation_schema.from_node == right_table_name
                            && relation_schema.to_node == left_table_name)
                    {
                        let rel_table_name = &relation_schema.table_name;
                        return Ok((
                            left_table_name,
                            rel_table_name.to_string(),
                            right_table_name,
                        ));
                    }
                }
            }
            // only left node is extracted but not able to extract the right node
            else if extracted_left_node_table_result.is_some()
                && extracted_right_node_table_result.is_none()
            {
                let left_table_name = extracted_left_node_table_result.unwrap();
                for (_, relation_schema) in graph_schema.get_relationships_schemas().iter() {
                    if relation_schema.from_node == left_table_name {
                        let right_table_name = &graph_schema
                            .get_node_schema(&relation_schema.to_node)
                            .map_err(|e| AnalyzerError::GraphSchema {
                                pass: Pass::SchemaInference,
                                source: e,
                            })?
                            .table_name;
                        let rel_table_name = &relation_schema.table_name;
                        return Ok((
                            left_table_name,
                            rel_table_name.to_string(),
                            right_table_name.to_string(),
                        ));
                    } else if relation_schema.to_node == left_table_name {
                        let right_table_name = &graph_schema
                            .get_node_schema(&relation_schema.from_node)
                            .map_err(|e| AnalyzerError::GraphSchema {
                                pass: Pass::SchemaInference,
                                source: e,
                            })?
                            .table_name;
                        let rel_table_name = &relation_schema.table_name;
                        return Ok((
                            left_table_name,
                            rel_table_name.to_string(),
                            right_table_name.to_string(),
                        ));
                    }
                }
            }
            // only right node is extracted but not able to extract the left node
            else if extracted_left_node_table_result.is_none()
                && extracted_right_node_table_result.is_some()
            {
                let right_table_name = extracted_right_node_table_result.unwrap();
                for (_, relation_schema) in graph_schema.get_relationships_schemas().iter() {
                    if relation_schema.from_node == right_table_name {
                        let left_table_name = &graph_schema
                            .get_node_schema(&relation_schema.to_node)
                            .map_err(|e| AnalyzerError::GraphSchema {
                                pass: Pass::SchemaInference,
                                source: e,
                            })?
                            .table_name;
                        let rel_table_name = &relation_schema.table_name;
                        return Ok((
                            left_table_name.to_string(),
                            rel_table_name.to_string(),
                            right_table_name,
                        ));
                    } else if relation_schema.to_node == right_table_name {
                        let left_table_name = &graph_schema
                            .get_node_schema(&relation_schema.from_node)
                            .map_err(|e| AnalyzerError::GraphSchema {
                                pass: Pass::SchemaInference,
                                source: e,
                            })?
                            .table_name;
                        let rel_table_name = &relation_schema.table_name;
                        return Ok((
                            left_table_name.to_string(),
                            rel_table_name.to_string(),
                            right_table_name,
                        ));
                    }
                }
            }
        }

        Err(AnalyzerError::NotEnoughLabels {
            pass: Pass::SchemaInference,
        })
    }

    fn get_table_name_from_filters_and_projections(
        &self,
        graph_schema: &GraphSchema,
        node_table_ctx: &TableCtx,
    ) -> Option<String> {
        let column_name = if let Some(extracted_column) =
            self.get_column_name_from_filter_predicates(node_table_ctx.get_filters())
        {
            extracted_column
        } else if let Some(extracted_column) =
            self.get_column_name_from_projection_items(node_table_ctx.get_projections())
        {
            extracted_column
        } else {
            "".to_string()
        };
        if !column_name.is_empty() {
            for (_, node_schema) in graph_schema.get_nodes_schemas().iter() {
                if node_schema.column_names.contains(&column_name) {
                    return Some(node_schema.table_name.clone());
                }
            }
        }
        None
    }

    fn get_column_name_from_filter_predicates(
        &self,
        filter_predicates: &[LogicalExpr],
    ) -> Option<String> {
        for filter_predicate in filter_predicates.iter() {
            if let LogicalExpr::OperatorApplicationExp(op_app) = filter_predicate {
                for operand in &op_app.operands {
                    if let Some(column_name) = Self::get_column_name_from_plan_expr(operand) {
                        return Some(column_name);
                    }
                }
            }
        }
        None
    }

    fn get_column_name_from_projection_items(
        &self,
        projection_items: &[ProjectionItem],
    ) -> Option<String> {
        for projection_item in projection_items.iter() {
            if let Some(column_name) =
                Self::get_column_name_from_plan_expr(&projection_item.expression)
            {
                return Some(column_name);
            }
        }
        None
    }

    fn get_column_name_from_plan_expr(exp: &LogicalExpr) -> Option<String> {
        match exp {
            LogicalExpr::OperatorApplicationExp(op_ex) => {
                for operand in &op_ex.operands {
                    if let Some(column_name) = Self::get_column_name_from_plan_expr(operand) {
                        return Some(column_name);
                    }
                }
                None
            }
            LogicalExpr::ScalarFnCall(function_call) => {
                for arg in &function_call.args {
                    if let Some(column_name) = Self::get_column_name_from_plan_expr(arg) {
                        return Some(column_name);
                    }
                }
                None
            }
            LogicalExpr::AggregateFnCall(function_call) => {
                for arg in &function_call.args {
                    if let Some(column_name) = Self::get_column_name_from_plan_expr(arg) {
                        return Some(column_name);
                    }
                }
                None
            }
            LogicalExpr::PropertyAccessExp(property_access) => {
                Some(property_access.column.0.clone())
            }
            LogicalExpr::Column(col) => Some(col.to_string()),
            _ => None,
        }
    }
}
